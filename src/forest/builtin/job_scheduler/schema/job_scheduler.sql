CREATE TABLE job_type_catalog (
    scheduler_generation_id text NOT NULL,
    job_type text NOT NULL,
    job_version integer NOT NULL CHECK (job_version > 0),
    graph_id text NOT NULL,
    graph_revision bigint NOT NULL CHECK (graph_revision > 0),
    input_schema jsonb NOT NULL,
    input_schema_digest text NOT NULL,
    max_attempts integer NOT NULL CHECK (max_attempts BETWEEN 1 AND 32),
    timeout_seconds integer NOT NULL CHECK (timeout_seconds BETWEEN 1 AND 86400),
    max_concurrency integer NOT NULL CHECK (max_concurrency BETWEEN 1 AND 128),
    owner_component_id text NOT NULL,
    target_generation_id text NOT NULL,
    target_service_id text NOT NULL,
    target_service_version integer NOT NULL CHECK (target_service_version > 0),
    PRIMARY KEY (scheduler_generation_id, job_type, job_version)
);

CREATE TABLE job_type_snapshot (
    scheduler_generation_id text PRIMARY KEY,
    graph_id text NOT NULL,
    graph_revision bigint NOT NULL CHECK (graph_revision > 0),
    refreshed_at timestamptz NOT NULL DEFAULT clock_timestamp()
);

CREATE TABLE jobs (
    bot_id text NOT NULL,
    job_id text NOT NULL,
    enqueue_operation_id text NOT NULL,
    job_type text NOT NULL,
    job_version integer NOT NULL CHECK (job_version > 0),
    requested_by text NOT NULL,
    source_conversation_ref text,
    input_value jsonb NOT NULL,
    state text NOT NULL CHECK (state IN ('QUEUED','RUNNING','SUCCEEDED','FAILED','CANCELLED','UNKNOWN')),
    revision bigint NOT NULL DEFAULT 1 CHECK (revision > 0),
    attempt_count integer NOT NULL DEFAULT 0 CHECK (attempt_count >= 0),
    max_attempts integer NOT NULL CHECK (max_attempts BETWEEN 1 AND 32),
    available_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    claim_token text,
    lease_until timestamptz,
    target_component_id text,
    target_generation_id text,
    target_service_id text,
    target_service_version integer,
    result_value jsonb,
    error_code text,
    cancel_requested_at timestamptz,
    cancel_requested_by text,
    cancel_reason_code text,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    completed_at timestamptz,
    PRIMARY KEY (bot_id, job_id),
    UNIQUE (bot_id, enqueue_operation_id)
);

CREATE INDEX jobs_queue_idx ON jobs(bot_id, state, available_at, created_at, job_id);
CREATE INDEX jobs_query_idx ON jobs(bot_id, created_at DESC, job_id DESC);
CREATE INDEX jobs_running_type_idx ON jobs(bot_id, job_type, job_version) WHERE state='RUNNING';

CREATE TABLE enqueue_operations (
    bot_id text NOT NULL,
    operation_id text NOT NULL,
    input_payload jsonb NOT NULL,
    result_payload jsonb NOT NULL,
    PRIMARY KEY (bot_id, operation_id)
);

CREATE TABLE cancellation_requests (
    bot_id text NOT NULL,
    operation_id text NOT NULL,
    input_payload jsonb NOT NULL,
    result_payload jsonb NOT NULL,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (bot_id, operation_id)
);

CREATE TABLE work_settlements (
    claim_token text PRIMARY KEY,
    input_payload jsonb NOT NULL,
    result_payload jsonb NOT NULL
);

CREATE FUNCTION job_projection(item __forest_module_schema__.jobs)
RETURNS jsonb LANGUAGE sql STABLE SET search_path=pg_catalog AS $$
SELECT jsonb_build_object(
  'job_id',item.job_id,'job_type',item.job_type,'job_version',item.job_version,
  'state',item.state,'revision',item.revision,'requested_by',item.requested_by,
  'source_conversation_ref',item.source_conversation_ref,'input',item.input_value,
  'result',item.result_value,'attempt_count',item.attempt_count,'max_attempts',item.max_attempts,
  'error_code',item.error_code,'cancel_requested_at',item.cancel_requested_at,
  'created_at',item.created_at,'updated_at',item.updated_at,'completed_at',item.completed_at,
  'target_component_id',item.target_component_id,
  'target_generation_id',item.target_generation_id,
  'target_service_id',item.target_service_id,
  'target_service_version',item.target_service_version
)
$$;

CREATE FUNCTION job_types_synchronize(context jsonb, input jsonb)
RETURNS jsonb LANGUAGE plpgsql VOLATILE SET search_path=pg_catalog AS $$
DECLARE entry jsonb; current_snapshot record;
        scheduler_generation text:=context->>'generation_id';
BEGIN
  IF input->>'protocol' IS DISTINCT FROM 'forest.contribution-directory/1'
     OR input->>'kind' IS DISTINCT FROM 'job-types'
     OR jsonb_typeof(input->'items') IS DISTINCT FROM 'array' THEN
    RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,'error',
      jsonb_build_object('code','JOB_DIRECTORY_INVALID','message','Job type directory is invalid','status',422));
  END IF;
  PERFORM pg_advisory_xact_lock(hashtextextended(
    'forest.job.catalog/'||(context->>'bot_id')||'/'||scheduler_generation,0));
  SELECT * INTO current_snapshot FROM __forest_module_schema__.job_type_snapshot
   WHERE scheduler_generation_id=scheduler_generation;
  IF NOT FOUND OR current_snapshot.graph_id IS DISTINCT FROM input->>'graph_id'
     OR current_snapshot.graph_revision IS DISTINCT FROM (input->>'graph_revision')::bigint THEN
    DELETE FROM __forest_module_schema__.job_type_catalog
     WHERE scheduler_generation_id=scheduler_generation;
    FOR entry IN SELECT value FROM jsonb_array_elements(input->'items') LOOP
      INSERT INTO __forest_module_schema__.job_type_catalog(
        scheduler_generation_id,job_type,job_version,graph_id,graph_revision,
        input_schema,input_schema_digest,max_attempts,timeout_seconds,max_concurrency,
        owner_component_id,target_generation_id,target_service_id,target_service_version)
      VALUES(scheduler_generation,entry->>'job_type',(entry->>'job_version')::integer,
        input->>'graph_id',(input->>'graph_revision')::bigint,entry->'input_schema',
        entry->>'input_schema_digest',(entry->>'max_attempts')::integer,
        (entry->>'timeout_seconds')::integer,(entry->>'max_concurrency')::integer,
        entry->>'owner_component_id',entry->>'target_generation_id',entry->>'service_id',
        (entry->>'service_version')::integer);
    END LOOP;
    INSERT INTO __forest_module_schema__.job_type_snapshot(
      scheduler_generation_id,graph_id,graph_revision,refreshed_at)
    VALUES(scheduler_generation,input->>'graph_id',(input->>'graph_revision')::bigint,
      clock_timestamp())
    ON CONFLICT(scheduler_generation_id) DO UPDATE SET graph_id=EXCLUDED.graph_id,
      graph_revision=EXCLUDED.graph_revision,refreshed_at=EXCLUDED.refreshed_at;
  END IF;
  RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',true,'value',
    jsonb_build_object('graph_id',input->>'graph_id',
      'graph_revision',(input->>'graph_revision')::bigint,
      'item_count',jsonb_array_length(input->'items')));
END;
$$;

CREATE FUNCTION job_types_cleanup(context jsonb, input jsonb)
RETURNS jsonb LANGUAGE plpgsql VOLATILE SET search_path=pg_catalog AS $$
DECLARE current_snapshot record;
        scheduler_generation text:=context->>'generation_id';
        released boolean:=false;
BEGIN
  IF input->>'protocol' IS DISTINCT FROM 'forest.contribution-directory-cleanup/1'
     OR input->>'kind' IS DISTINCT FROM 'job-types' THEN
    RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,'error',
      jsonb_build_object('code','JOB_DIRECTORY_CLEANUP_INVALID',
        'message','Job type directory cleanup is invalid','status',422));
  END IF;
  PERFORM pg_advisory_xact_lock(hashtextextended(
    'forest.job.catalog/'||(context->>'bot_id')||'/'||scheduler_generation,0));
  SELECT * INTO current_snapshot FROM __forest_module_schema__.job_type_snapshot
   WHERE scheduler_generation_id=scheduler_generation FOR UPDATE;
  IF FOUND AND (current_snapshot.graph_id IS DISTINCT FROM input->>'graph_id'
     OR current_snapshot.graph_revision IS DISTINCT FROM (input->>'graph_revision')::bigint) THEN
    RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,'error',
      jsonb_build_object('code','JOB_DIRECTORY_OWNERSHIP_CONFLICT',
        'message','Job type directory belongs to another RuntimeGraph','status',409));
  END IF;
  released:=FOUND;
  DELETE FROM __forest_module_schema__.job_type_catalog
   WHERE scheduler_generation_id=scheduler_generation;
  DELETE FROM __forest_module_schema__.job_type_snapshot
   WHERE scheduler_generation_id=scheduler_generation;
  RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',true,'value',
    jsonb_build_object('released',released));
END;
$$;

CREATE FUNCTION jobs_types(context jsonb, input jsonb)
RETURNS jsonb LANGUAGE plpgsql STABLE SET search_path=pg_catalog AS $$
DECLARE snapshot record; values_json jsonb;
BEGIN
  SELECT * INTO snapshot FROM __forest_module_schema__.job_type_snapshot
   WHERE scheduler_generation_id=context->>'generation_id';
  SELECT COALESCE(jsonb_agg(jsonb_build_object(
    'job_type',c.job_type,'job_version',c.job_version,'input_schema',c.input_schema,
    'input_schema_digest',c.input_schema_digest,'max_attempts',c.max_attempts,
    'timeout_seconds',c.timeout_seconds,'max_concurrency',c.max_concurrency,
    'owner_component_id',c.owner_component_id,'target_generation_id',c.target_generation_id,
    'target_service_id',c.target_service_id,'target_service_version',c.target_service_version
  ) ORDER BY c.job_type,c.job_version),'[]'::jsonb) INTO values_json
  FROM __forest_module_schema__.job_type_catalog c
   WHERE c.scheduler_generation_id=context->>'generation_id';
  RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',true,'value',
    jsonb_build_object('graph_id',snapshot.graph_id,'graph_revision',snapshot.graph_revision,
      'items',values_json));
END;
$$;

CREATE FUNCTION jobs_read(context jsonb, input jsonb)
RETURNS jsonb LANGUAGE plpgsql STABLE SET search_path=pg_catalog AS $$
DECLARE item __forest_module_schema__.jobs%ROWTYPE;
BEGIN
  SELECT * INTO item FROM __forest_module_schema__.jobs
   WHERE bot_id=context->>'bot_id' AND job_id=input->>'job_id';
  IF NOT FOUND THEN
    RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,'error',
      jsonb_build_object('code','JOB_NOT_FOUND','message','Job does not exist','status',404));
  END IF;
  RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',true,'value',
    __forest_module_schema__.job_projection(item));
END;
$$;

CREATE FUNCTION jobs_query(context jsonb, input jsonb)
RETURNS jsonb LANGUAGE plpgsql STABLE SET search_path=pg_catalog AS $$
DECLARE requested_limit integer:=COALESCE((input->>'limit')::integer,50);
        snapshot_time timestamptz:=COALESCE((input->>'snapshot_created_at')::timestamptz,statement_timestamp());
        before_time timestamptz; before_id text; rows_json jsonb; row_count integer;
BEGIN
  IF (input ? 'before_created_at') IS DISTINCT FROM (input ? 'before_job_id') THEN
    RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,'error',
      jsonb_build_object('code','JOB_CURSOR_INVALID','message','Job cursor is incomplete','status',422));
  END IF;
  before_time:=(input->>'before_created_at')::timestamptz;
  before_id:=input->>'before_job_id';
  WITH selected AS (
    SELECT j.* FROM __forest_module_schema__.jobs j
     WHERE j.bot_id=context->>'bot_id' AND j.created_at<=snapshot_time
       AND (NOT input ? 'states' OR j.state IN (SELECT jsonb_array_elements_text(input->'states')))
       AND (NOT input ? 'job_type' OR j.job_type=input->>'job_type')
       AND (NOT input ? 'requested_by' OR j.requested_by=input->>'requested_by')
       AND (before_time IS NULL OR (j.created_at,j.job_id)<(before_time,before_id))
     ORDER BY j.created_at DESC,j.job_id DESC LIMIT requested_limit+1
  ), projected AS (
    SELECT __forest_module_schema__.job_projection(selected) AS value,
           created_at,job_id,row_number() OVER (ORDER BY created_at DESC,job_id DESC) AS position
      FROM selected
  )
  SELECT COALESCE(jsonb_agg(value ORDER BY position) FILTER (WHERE position<=requested_limit),'[]'::jsonb),
         count(*) INTO rows_json,row_count FROM projected;
  RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',true,'value',
    jsonb_build_object('snapshot_created_at',snapshot_time,'items',rows_json,
      'has_more',row_count>requested_limit,
      'next_before_created_at',CASE WHEN row_count>requested_limit THEN
        (rows_json->(requested_limit-1)->>'created_at')::timestamptz ELSE NULL END,
      'next_before_job_id',CASE WHEN row_count>requested_limit THEN
        rows_json->(requested_limit-1)->>'job_id' ELSE NULL END));
END;
$$;

CREATE FUNCTION jobs_enqueue(context jsonb, input jsonb)
RETURNS jsonb LANGUAGE plpgsql VOLATILE SET search_path=pg_catalog AS $$
DECLARE existing record; selected __forest_module_schema__.job_type_catalog%ROWTYPE;
        saved __forest_module_schema__.jobs%ROWTYPE; result_value jsonb;
        requested_operation text:=context->>'operation_id'; requested_bot text:=context->>'bot_id';
BEGIN
  IF requested_operation IS NULL OR octet_length(requested_operation) NOT BETWEEN 1 AND 256
     OR octet_length(input::text)>200000 THEN
    RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,'error',
      jsonb_build_object('code','JOB_INPUT_INVALID','message','Job request is invalid','status',422));
  END IF;
  PERFORM pg_advisory_xact_lock(hashtextextended('forest.job.enqueue/'||requested_bot||'/'||requested_operation,0));
  SELECT * INTO existing FROM __forest_module_schema__.enqueue_operations
   WHERE bot_id=requested_bot AND operation_id=requested_operation;
  IF FOUND THEN
    IF existing.input_payload=input THEN RETURN existing.result_payload; END IF;
    RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,'error',
      jsonb_build_object('code','JOB_OPERATION_CONFLICT','message','Job operation input differs','status',409));
  END IF;
  SELECT * INTO selected FROM __forest_module_schema__.job_type_catalog
   WHERE scheduler_generation_id=context->>'generation_id'
     AND job_type=input->>'job_type' ORDER BY job_version DESC LIMIT 1;
  IF NOT FOUND THEN
    RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,'error',
      jsonb_build_object('code','JOB_TYPE_NOT_FOUND','message','Job type is not available','status',404));
  END IF;
  INSERT INTO __forest_module_schema__.jobs(
    bot_id,job_id,enqueue_operation_id,job_type,job_version,requested_by,
    source_conversation_ref,input_value,state,max_attempts)
  VALUES(requested_bot,'job:'||requested_operation,requested_operation,selected.job_type,
    selected.job_version,input->>'requested_by',input->>'source_conversation_ref',input->'input',
    'QUEUED',selected.max_attempts) RETURNING * INTO saved;
  result_value:=jsonb_build_object('protocol','forest.domain-result/1','ok',true,'value',
    __forest_module_schema__.job_projection(saved));
  INSERT INTO __forest_module_schema__.enqueue_operations(bot_id,operation_id,input_payload,result_payload)
    VALUES(requested_bot,requested_operation,input,result_value);
  RETURN result_value;
END;
$$;

CREATE FUNCTION jobs_cancel(context jsonb, input jsonb)
RETURNS jsonb LANGUAGE plpgsql VOLATILE SET search_path=pg_catalog AS $$
DECLARE existing record; item __forest_module_schema__.jobs%ROWTYPE; result_value jsonb;
        requested_operation text:=context->>'operation_id'; requested_bot text:=context->>'bot_id';
BEGIN
  PERFORM pg_advisory_xact_lock(hashtextextended('forest.job.cancel/'||requested_bot||'/'||input->>'job_id',0));
  SELECT * INTO existing FROM __forest_module_schema__.cancellation_requests
   WHERE bot_id=requested_bot AND operation_id=requested_operation;
  IF FOUND THEN
    IF existing.input_payload=input THEN RETURN existing.result_payload; END IF;
    RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,'error',
      jsonb_build_object('code','JOB_OPERATION_CONFLICT','message','Cancellation input differs','status',409));
  END IF;
  SELECT * INTO item FROM __forest_module_schema__.jobs
   WHERE bot_id=requested_bot AND job_id=input->>'job_id' FOR UPDATE;
  IF NOT FOUND THEN RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,'error',
    jsonb_build_object('code','JOB_NOT_FOUND','message','Job does not exist','status',404)); END IF;
  IF item.revision<>(input->>'expected_revision')::bigint THEN
    RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,'error',
      jsonb_build_object('code','JOB_REVISION_CONFLICT','message','Job revision changed','status',409,
        'details',jsonb_build_object('revision',item.revision)));
  END IF;
  IF item.state='QUEUED' THEN
    UPDATE __forest_module_schema__.jobs SET state='CANCELLED',revision=revision+1,
      cancel_requested_at=clock_timestamp(),cancel_requested_by=input->>'requested_by',
      cancel_reason_code=COALESCE(input->>'reason_code','ADMIN_REQUESTED'),
      completed_at=clock_timestamp(),updated_at=clock_timestamp()
     WHERE bot_id=requested_bot AND job_id=item.job_id RETURNING * INTO item;
  ELSIF item.state='RUNNING' AND item.cancel_requested_at IS NULL THEN
    UPDATE __forest_module_schema__.jobs SET revision=revision+1,cancel_requested_at=clock_timestamp(),
      cancel_requested_by=input->>'requested_by',
      cancel_reason_code=COALESCE(input->>'reason_code','ADMIN_REQUESTED'),updated_at=clock_timestamp()
     WHERE bot_id=requested_bot AND job_id=item.job_id RETURNING * INTO item;
  ELSE
    RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,'error',
      jsonb_build_object('code','JOB_CANCELLATION_CONFLICT','message','Job cannot accept cancellation','status',409));
  END IF;
  result_value:=jsonb_build_object('protocol','forest.domain-result/1','ok',true,'value',
    jsonb_build_object('operation_id',requested_operation,'job_id',item.job_id,
      'resulting_state',item.state,'resulting_revision',item.revision,'created_at',clock_timestamp()));
  INSERT INTO __forest_module_schema__.cancellation_requests(bot_id,operation_id,input_payload,result_payload)
    VALUES(requested_bot,requested_operation,input,result_value);
  RETURN result_value;
END;
$$;

CREATE FUNCTION job_cancellation_requested(context jsonb, input jsonb)
RETURNS jsonb LANGUAGE plpgsql STABLE SET search_path=pg_catalog AS $$
DECLARE requested boolean;
BEGIN
  SELECT cancel_requested_at IS NOT NULL INTO requested FROM __forest_module_schema__.jobs
   WHERE bot_id=context->>'bot_id' AND job_id=input->>'job_id' AND state='RUNNING'
     AND claim_token=input->>'claim_token' AND lease_until>statement_timestamp();
  IF NOT FOUND THEN RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,'error',
    jsonb_build_object('code','STALE_JOB_CLAIM','message','Job claim is no longer owned','status',409)); END IF;
  RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',true,'value',
    jsonb_build_object('requested',requested));
END;
$$;

CREATE FUNCTION job_claim_work(context jsonb, input jsonb)
RETURNS jsonb LANGUAGE plpgsql VOLATILE SET search_path=pg_catalog AS $$
DECLARE selected __forest_module_schema__.jobs%ROWTYPE;
        route __forest_module_schema__.job_type_catalog%ROWTYPE;
        requested_token text:=input->>'claim_token'; requested_lease integer:=(input->>'lease_seconds')::integer;
BEGIN
  IF input->>'protocol' IS DISTINCT FROM 'forest.owned-work-claim-request/1'
  THEN
    RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,'error',
      jsonb_build_object('code','JOB_CLAIM_INVALID','message','Job claim request is invalid','status',422));
  END IF;
  UPDATE __forest_module_schema__.jobs SET state='UNKNOWN',revision=revision+1,claim_token=NULL,
    lease_until=NULL,error_code='JOB_RESULT_UNKNOWN_AFTER_LEASE',
    completed_at=clock_timestamp(),updated_at=clock_timestamp()
   WHERE bot_id=context->>'bot_id' AND state='RUNNING' AND lease_until<=clock_timestamp();
  UPDATE __forest_module_schema__.jobs queued SET state='FAILED',revision=queued.revision+1,
    error_code='JOB_TYPE_RETIRED',completed_at=clock_timestamp(),updated_at=clock_timestamp()
   WHERE queued.bot_id=context->>'bot_id' AND queued.state='QUEUED'
     AND NOT EXISTS (
       SELECT 1 FROM __forest_module_schema__.job_type_catalog current_type
        WHERE current_type.scheduler_generation_id=context->>'generation_id'
          AND current_type.job_type=queued.job_type
          AND current_type.job_version=queued.job_version);
  SELECT j.* INTO selected FROM __forest_module_schema__.jobs j
   JOIN __forest_module_schema__.job_type_catalog c
     ON c.job_type=j.job_type AND c.job_version=j.job_version
   WHERE j.bot_id=context->>'bot_id' AND j.state='QUEUED' AND j.available_at<=clock_timestamp()
     AND c.scheduler_generation_id=context->>'generation_id'
     AND (SELECT count(*) FROM __forest_module_schema__.jobs active
       WHERE active.bot_id=j.bot_id AND active.state='RUNNING'
         AND active.job_type=j.job_type AND active.job_version=j.job_version)<c.max_concurrency
   ORDER BY j.available_at,j.created_at,j.job_id FOR UPDATE OF j SKIP LOCKED LIMIT 1;
  IF NOT FOUND THEN RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',true,'value',NULL); END IF;
  SELECT * INTO route FROM __forest_module_schema__.job_type_catalog
   WHERE scheduler_generation_id=context->>'generation_id'
     AND job_type=selected.job_type AND job_version=selected.job_version;
  UPDATE __forest_module_schema__.jobs SET state='RUNNING',revision=revision+1,
    attempt_count=attempt_count+1,claim_token=requested_token,
    lease_until=clock_timestamp()+requested_lease*interval '1 second',
    max_attempts=route.max_attempts,target_component_id=route.owner_component_id,
    target_generation_id=route.target_generation_id,target_service_id=route.target_service_id,
    target_service_version=route.target_service_version,error_code=NULL,updated_at=clock_timestamp()
   WHERE bot_id=selected.bot_id AND job_id=selected.job_id RETURNING * INTO selected;
  RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',true,'value',
    jsonb_build_object('protocol','forest.owned-work-claim/1','work_id',selected.job_id,
      'claim_token',selected.claim_token,'attempt',selected.attempt_count,
      'operation_id','job.execute:'||selected.job_id||':'||selected.attempt_count,
      'causal_root_kind','job.scheduler','causal_root_id',selected.job_id,
      'payload',jsonb_build_object('job_id',selected.job_id,'job_type',selected.job_type,
        'job_version',selected.job_version,'requested_by',selected.requested_by,
        'input',selected.input_value,'claim_token',selected.claim_token,
        'max_attempts',selected.max_attempts,'timeout_seconds',route.timeout_seconds,
        'target_component_id',selected.target_component_id,
        'target_generation_id',selected.target_generation_id,
        'target_service_id',selected.target_service_id,
        'target_service_version',selected.target_service_version)));
END;
$$;

CREATE FUNCTION job_settle_work(context jsonb, input jsonb)
RETURNS jsonb LANGUAGE plpgsql VOLATILE SET search_path=pg_catalog AS $$
DECLARE existing record; item __forest_module_schema__.jobs%ROWTYPE; work_result jsonb;
        disposition text; next_state text; settlement_envelope jsonb; can_retry boolean;
BEGIN
  SELECT * INTO existing FROM __forest_module_schema__.work_settlements
   WHERE claim_token=input->>'claim_token';
  IF FOUND THEN
    IF existing.input_payload=input THEN RETURN existing.result_payload; END IF;
    RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,'error',
      jsonb_build_object('code','JOB_SETTLEMENT_CONFLICT','message','Claim has another settlement','status',409));
  END IF;
  SELECT * INTO item FROM __forest_module_schema__.jobs
   WHERE bot_id=context->>'bot_id' AND job_id=input->>'work_id' AND state='RUNNING'
     AND claim_token=input->>'claim_token' AND lease_until>clock_timestamp() FOR UPDATE;
  IF NOT FOUND THEN RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,'error',
    jsonb_build_object('code','STALE_JOB_CLAIM','message','Job claim is no longer owned','status',409)); END IF;
  IF item.cancel_requested_at IS NOT NULL THEN
    disposition:='CANCELLED'; work_result:=NULL;
  ELSIF input->'outcome'->>'status'='SUCCEEDED' THEN
    work_result:=input->'outcome'->'result'; disposition:=work_result->>'disposition';
  ELSE
    work_result:=NULL;
    can_retry:=COALESCE((input->'outcome'->'error'->>'retryable')::boolean,false)
      AND item.attempt_count<item.max_attempts;
    disposition:=CASE WHEN can_retry THEN 'RETRY' ELSE 'FAILED' END;
  END IF;
  next_state:=CASE disposition WHEN 'SUCCEEDED' THEN 'SUCCEEDED' WHEN 'RETRY' THEN 'QUEUED'
    WHEN 'CANCELLED' THEN 'CANCELLED' WHEN 'UNKNOWN' THEN 'UNKNOWN' ELSE 'FAILED' END;
  UPDATE __forest_module_schema__.jobs AS current_job SET state=next_state,revision=current_job.revision+1,
    claim_token=NULL,lease_until=NULL,target_component_id=NULL,target_generation_id=NULL,
    target_service_id=NULL,target_service_version=NULL,
    result_value=CASE WHEN disposition='SUCCEEDED' THEN work_result->'result' ELSE current_job.result_value END,
    error_code=CASE WHEN disposition='SUCCEEDED' THEN NULL WHEN disposition='CANCELLED' THEN 'JOB_CANCELLED'
      WHEN work_result IS NOT NULL THEN work_result->>'error_code'
      ELSE input->'outcome'->'error'->>'code' END,
    available_at=CASE WHEN disposition='RETRY' THEN clock_timestamp()+
      COALESCE((work_result->>'retry_delay_seconds')::integer,1)*interval '1 second' ELSE available_at END,
    completed_at=CASE WHEN disposition='RETRY' THEN NULL ELSE clock_timestamp() END,
    updated_at=clock_timestamp()
   WHERE current_job.bot_id=item.bot_id AND current_job.job_id=item.job_id RETURNING * INTO item;
  settlement_envelope:=jsonb_build_object('protocol','forest.domain-result/1','ok',true,'value',
    jsonb_build_object('protocol','forest.owned-work-settled/1','work_id',item.job_id));
  INSERT INTO __forest_module_schema__.work_settlements(claim_token,input_payload,result_payload)
    VALUES(input->>'claim_token',input,settlement_envelope);
  RETURN settlement_envelope;
END;
$$;
