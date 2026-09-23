CREATE TABLE routing_directories (
    bot_id text NOT NULL REFERENCES forest_kernel.bot_identity(bot_id),
    routing_generation_id text NOT NULL,
    graph_id text NOT NULL,
    graph_revision bigint NOT NULL CHECK (graph_revision > 0),
    directory_digest text NOT NULL CHECK (directory_digest ~ '^md5:[a-f0-9]{32}$'),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (bot_id, routing_generation_id),
    UNIQUE (bot_id, graph_id)
);

CREATE TABLE event_publication_routes (
    bot_id text NOT NULL,
    routing_generation_id text NOT NULL,
    publisher_component_id text NOT NULL,
    publisher_generation_id text NOT NULL,
    topic text NOT NULL,
    contract text NOT NULL,
    event_schema_digest text NOT NULL,
    PRIMARY KEY (bot_id,routing_generation_id,publisher_component_id,topic,contract),
    FOREIGN KEY (bot_id,routing_generation_id)
      REFERENCES routing_directories(bot_id,routing_generation_id)
);

CREATE TABLE event_subscription_routes (
    bot_id text NOT NULL,
    routing_generation_id text NOT NULL,
    subscriber_id text NOT NULL,
    contract_major integer NOT NULL CHECK (contract_major > 0),
    topic text NOT NULL,
    contract text NOT NULL,
    event_schema_digest text NOT NULL,
    owner_component_id text NOT NULL,
    owner_generation_id text NOT NULL,
    handler_service_id text NOT NULL,
    handler_service_version integer NOT NULL CHECK (handler_service_version > 0),
    max_attempts integer NOT NULL CHECK (max_attempts BETWEEN 1 AND 32),
    PRIMARY KEY (bot_id,routing_generation_id,subscriber_id,contract_major),
    FOREIGN KEY (bot_id,routing_generation_id)
      REFERENCES routing_directories(bot_id,routing_generation_id),
    UNIQUE (bot_id,routing_generation_id,topic,contract,subscriber_id)
);

CREATE INDEX event_subscription_topic_idx ON event_subscription_routes(
    bot_id,routing_generation_id,topic,contract,subscriber_id
);

CREATE TABLE component_job_routes (
    bot_id text NOT NULL,
    routing_generation_id text NOT NULL,
    owner_component_id text NOT NULL,
    owner_generation_id text NOT NULL,
    handler_id text NOT NULL,
    input_schema_digest text NOT NULL,
    handler_service_id text NOT NULL,
    handler_service_version integer NOT NULL CHECK (handler_service_version > 0),
    max_attempts integer NOT NULL CHECK (max_attempts BETWEEN 1 AND 32),
    timeout_seconds double precision NOT NULL CHECK (timeout_seconds > 0 AND timeout_seconds <= 120),
    max_concurrency integer NOT NULL CHECK (max_concurrency BETWEEN 1 AND 16),
    PRIMARY KEY (bot_id,routing_generation_id,owner_component_id,handler_id),
    FOREIGN KEY (bot_id,routing_generation_id)
      REFERENCES routing_directories(bot_id,routing_generation_id)
);

CREATE TABLE component_events (
    bot_id text NOT NULL REFERENCES forest_kernel.bot_identity(bot_id),
    event_id text NOT NULL,
    operation_id text NOT NULL,
    input_digest text NOT NULL,
    publisher_component_id text NOT NULL,
    publisher_generation_id text NOT NULL,
    graph_id text NOT NULL,
    topic text NOT NULL,
    contract text NOT NULL,
    event_schema_digest text NOT NULL,
    payload jsonb NOT NULL,
    causal_root_kind text NOT NULL,
    causal_root_id text NOT NULL,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (bot_id,event_id),
    UNIQUE (bot_id,publisher_component_id,operation_id)
);

CREATE TABLE event_deliveries (
    bot_id text NOT NULL,
    delivery_id text NOT NULL,
    event_id text NOT NULL,
    subscriber_id text NOT NULL,
    contract_major integer NOT NULL,
    topic text NOT NULL,
    contract text NOT NULL,
    event_schema_digest text NOT NULL,
    owner_component_id text NOT NULL,
    handler_service_id text NOT NULL,
    handler_service_version integer NOT NULL,
    max_attempts integer NOT NULL CHECK (max_attempts BETWEEN 1 AND 32),
    state text NOT NULL DEFAULT 'QUEUED' CHECK (
      state IN ('QUEUED','CLAIMED','RETRY_WAIT','SUCCEEDED','DEAD_LETTER','UNKNOWN')
    ),
    revision bigint NOT NULL DEFAULT 1 CHECK (revision > 0),
    attempt_count integer NOT NULL DEFAULT 0 CHECK (attempt_count BETWEEN 0 AND max_attempts),
    claim_token text,
    routing_generation_id text,
    target_generation_id text,
    active_attempt_id text,
    lease_until timestamptz,
    available_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    error_code text,
    completed_at timestamptz,
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (bot_id,delivery_id),
    UNIQUE (bot_id,event_id,subscriber_id,contract_major),
    FOREIGN KEY (bot_id,event_id) REFERENCES component_events(bot_id,event_id),
    CHECK ((state='CLAIMED') = (claim_token IS NOT NULL)),
    CHECK ((state='CLAIMED') = (routing_generation_id IS NOT NULL)),
    CHECK ((state='CLAIMED') = (target_generation_id IS NOT NULL)),
    CHECK ((state='CLAIMED') = (active_attempt_id IS NOT NULL)),
    CHECK ((state='CLAIMED') = (lease_until IS NOT NULL))
);

CREATE INDEX event_delivery_queue_idx ON event_deliveries(
    bot_id,state,available_at,updated_at,delivery_id
);

CREATE TABLE event_delivery_attempts (
    bot_id text NOT NULL,
    attempt_id text NOT NULL,
    delivery_id text NOT NULL,
    attempt_no integer NOT NULL CHECK (attempt_no > 0),
    claim_token text NOT NULL,
    routing_generation_id text NOT NULL,
    target_generation_id text NOT NULL,
    state text NOT NULL CHECK (state IN ('DISPATCHING','SUCCEEDED','FAILED','UNKNOWN')),
    error_code text,
    started_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    completed_at timestamptz,
    PRIMARY KEY (bot_id,attempt_id),
    UNIQUE (bot_id,delivery_id,attempt_no),
    FOREIGN KEY (bot_id,delivery_id) REFERENCES event_deliveries(bot_id,delivery_id)
);

CREATE TABLE component_jobs (
    bot_id text NOT NULL REFERENCES forest_kernel.bot_identity(bot_id),
    job_id text NOT NULL,
    operation_id text NOT NULL,
    input_digest text NOT NULL,
    owner_component_id text NOT NULL,
    scheduled_by_generation_id text NOT NULL,
    graph_id text NOT NULL,
    handler_id text NOT NULL,
    input_schema_digest text NOT NULL,
    input_payload jsonb NOT NULL,
    causal_root_kind text NOT NULL,
    causal_root_id text NOT NULL,
    max_attempts integer NOT NULL CHECK (max_attempts BETWEEN 1 AND 32),
    timeout_seconds double precision NOT NULL CHECK (timeout_seconds > 0 AND timeout_seconds <= 120),
    max_concurrency integer NOT NULL CHECK (max_concurrency BETWEEN 1 AND 16),
    handler_service_id text NOT NULL,
    handler_service_version integer NOT NULL CHECK (handler_service_version > 0),
    state text NOT NULL DEFAULT 'WAITING' CHECK (
      state IN ('WAITING','CLAIMED','RETRY_WAIT','SUCCEEDED','FAILED','CANCELLED','UNKNOWN')
    ),
    revision bigint NOT NULL DEFAULT 1 CHECK (revision > 0),
    attempt_count integer NOT NULL DEFAULT 0 CHECK (attempt_count BETWEEN 0 AND max_attempts),
    claim_token text,
    routing_generation_id text,
    target_generation_id text,
    active_attempt_id text,
    lease_until timestamptz,
    available_at timestamptz NOT NULL,
    result_payload jsonb,
    error_code text,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    completed_at timestamptz,
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (bot_id,job_id),
    UNIQUE (bot_id,owner_component_id,operation_id),
    CHECK ((state='CLAIMED') = (claim_token IS NOT NULL)),
    CHECK ((state='CLAIMED') = (routing_generation_id IS NOT NULL)),
    CHECK ((state='CLAIMED') = (target_generation_id IS NOT NULL)),
    CHECK ((state='CLAIMED') = (active_attempt_id IS NOT NULL)),
    CHECK ((state='CLAIMED') = (lease_until IS NOT NULL)),
    CHECK ((state='SUCCEEDED') = (result_payload IS NOT NULL))
);

CREATE INDEX component_job_queue_idx ON component_jobs(
    bot_id,state,available_at,updated_at,job_id
);

CREATE TABLE component_job_attempts (
    bot_id text NOT NULL,
    attempt_id text NOT NULL,
    job_id text NOT NULL,
    attempt_no integer NOT NULL CHECK (attempt_no > 0),
    claim_token text NOT NULL,
    routing_generation_id text NOT NULL,
    target_generation_id text NOT NULL,
    state text NOT NULL CHECK (state IN ('DISPATCHING','SUCCEEDED','FAILED','UNKNOWN')),
    error_code text,
    started_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    completed_at timestamptz,
    PRIMARY KEY (bot_id,attempt_id),
    UNIQUE (bot_id,job_id,attempt_no),
    FOREIGN KEY (bot_id,job_id) REFERENCES component_jobs(bot_id,job_id)
);

CREATE TABLE work_settlements (
    claim_token text PRIMARY KEY,
    input_payload jsonb NOT NULL,
    result_payload jsonb NOT NULL
);

CREATE FUNCTION domain_ok(value jsonb) RETURNS jsonb
LANGUAGE sql IMMUTABLE SET search_path=pg_catalog AS $$
SELECT jsonb_build_object('protocol','forest.domain-result/1','ok',true,'value',value)
$$;

CREATE FUNCTION domain_error(code text,message text,status integer) RETURNS jsonb
LANGUAGE sql IMMUTABLE SET search_path=pg_catalog AS $$
SELECT jsonb_build_object('protocol','forest.domain-result/1','ok',false,'error',
  jsonb_build_object('code',code,'message',message,'status',status))
$$;

CREATE FUNCTION routing_directory_synchronize(context jsonb,input jsonb) RETURNS jsonb
LANGUAGE plpgsql VOLATILE SET search_path=pg_catalog AS $$
DECLARE item jsonb; existing text; digest text:='md5:'||md5(input::text);
BEGIN
  IF input->>'protocol' IS DISTINCT FROM 'forest.routing-directory/2'
     OR input->>'kind' IS DISTINCT FROM 'routing-routes' THEN
    RETURN __forest_module_schema__.domain_error('ROUTING_DIRECTORY_INVALID','Routing directory is invalid',422);
  END IF;
  PERFORM pg_advisory_xact_lock(hashtextextended(
    'forest.routing.directory/'||(context->>'bot_id')||'/'||(context->>'generation_id'),0));
  SELECT directory_digest INTO existing FROM __forest_module_schema__.routing_directories
   WHERE bot_id=context->>'bot_id' AND routing_generation_id=context->>'generation_id';
  IF existing IS NOT NULL THEN
    IF existing<>digest THEN
      RETURN __forest_module_schema__.domain_error('ROUTING_DIRECTORY_CONFLICT','Routing directory differs',409);
    END IF;
    RETURN __forest_module_schema__.domain_ok(jsonb_build_object('directory_digest',digest));
  END IF;
  INSERT INTO __forest_module_schema__.routing_directories(
    bot_id,routing_generation_id,graph_id,graph_revision,directory_digest)
  VALUES(context->>'bot_id',context->>'generation_id',input->>'graph_id',
    (input->>'graph_revision')::bigint,digest);
  FOR item IN SELECT value FROM jsonb_array_elements(input->'publications') LOOP
    INSERT INTO __forest_module_schema__.event_publication_routes VALUES(
      context->>'bot_id',context->>'generation_id',item->>'owner_component_id',
      item->>'owner_generation_id',item->>'topic',item->>'contract',item->>'event_schema_digest');
  END LOOP;
  FOR item IN SELECT value FROM jsonb_array_elements(input->'subscriptions') LOOP
    INSERT INTO __forest_module_schema__.event_subscription_routes VALUES(
      context->>'bot_id',context->>'generation_id',item->>'subscriber_id',
      (item->>'contract_major')::integer,item->>'topic',item->>'contract',
      item->>'event_schema_digest',item->>'owner_component_id',item->>'target_generation_id',
      item->>'service_id',(item->>'service_version')::integer,(item->>'max_attempts')::integer);
  END LOOP;
  FOR item IN SELECT value FROM jsonb_array_elements(input->'component_jobs') LOOP
    INSERT INTO __forest_module_schema__.component_job_routes VALUES(
      context->>'bot_id',context->>'generation_id',item->>'owner_component_id',
      item->>'target_generation_id',item->>'handler_id',item->>'input_schema_digest',
      item->>'service_id',(item->>'service_version')::integer,(item->>'max_attempts')::integer,
      (item->>'timeout_seconds')::double precision,(item->>'max_concurrency')::integer);
  END LOOP;
  IF EXISTS(
    SELECT 1 FROM __forest_module_schema__.event_subscription_routes s
    JOIN __forest_module_schema__.event_publication_routes p
      ON p.bot_id=s.bot_id AND p.routing_generation_id=s.routing_generation_id
     AND p.topic=s.topic AND p.contract=s.contract
    WHERE s.bot_id=context->>'bot_id' AND s.routing_generation_id=context->>'generation_id'
      AND s.event_schema_digest<>p.event_schema_digest
  ) THEN
    RETURN __forest_module_schema__.domain_error('ROUTING_DIRECTORY_CONFLICT','Event schemas differ',409);
  END IF;
  RETURN __forest_module_schema__.domain_ok(jsonb_build_object('directory_digest',digest));
EXCEPTION WHEN unique_violation THEN
  RETURN __forest_module_schema__.domain_error('ROUTING_DIRECTORY_CONFLICT','Routing identities are ambiguous',409);
END;
$$;

CREATE FUNCTION routing_directory_cleanup(context jsonb,input jsonb) RETURNS jsonb
LANGUAGE sql VOLATILE SET search_path=pg_catalog AS $$
SELECT __forest_module_schema__.domain_ok(jsonb_build_object('released',false))
$$;

CREATE FUNCTION event_publish(context jsonb,input jsonb) RETURNS jsonb
LANGUAGE plpgsql VOLATILE SET search_path=pg_catalog AS $$
DECLARE route record; previous record; item record; event_key text;
        input_hash text; delivery_total integer:=0; directory_graph text;
BEGIN
  input_hash:=md5(jsonb_build_object(
    'publisher_component_id',input->>'publisher_component_id','topic',input->>'topic',
    'contract',input->>'contract','payload',input->'payload')::text);
  event_key:='event:'||md5((context->>'bot_id')||chr(31)||(input->>'publisher_component_id')||chr(31)||(context->>'operation_id'));
  PERFORM pg_advisory_xact_lock(hashtextextended(event_key,0));
  SELECT event_id,input_digest INTO previous FROM __forest_module_schema__.component_events
   WHERE bot_id=context->>'bot_id' AND publisher_component_id=input->>'publisher_component_id'
     AND operation_id=context->>'operation_id';
  IF FOUND THEN
    IF previous.input_digest<>input_hash THEN
      RETURN __forest_module_schema__.domain_error('EVENT_OPERATION_CONFLICT','Event operation input differs',409);
    END IF;
    SELECT count(*) INTO delivery_total FROM __forest_module_schema__.event_deliveries
     WHERE bot_id=context->>'bot_id' AND event_id=previous.event_id;
    RETURN __forest_module_schema__.domain_ok(jsonb_build_object(
      'event_id',previous.event_id,'delivery_count',delivery_total,'replayed',true));
  END IF;
  SELECT p.*,d.graph_id INTO route FROM __forest_module_schema__.event_publication_routes p
   JOIN __forest_module_schema__.routing_directories d USING(bot_id,routing_generation_id)
   WHERE p.bot_id=context->>'bot_id' AND p.routing_generation_id=context->>'generation_id'
     AND p.publisher_component_id=input->>'publisher_component_id'
     AND p.publisher_generation_id=input->>'publisher_generation_id'
     AND p.topic=input->>'topic' AND p.contract=input->>'contract';
  IF NOT FOUND THEN
    RETURN __forest_module_schema__.domain_error('EVENT_ROUTE_UNAVAILABLE','Event publication route is unavailable',409);
  END IF;
  directory_graph:=route.graph_id;
  INSERT INTO __forest_module_schema__.component_events VALUES(
    context->>'bot_id',event_key,context->>'operation_id',input_hash,
    input->>'publisher_component_id',input->>'publisher_generation_id',directory_graph,
    input->>'topic',input->>'contract',route.event_schema_digest,input->'payload',
    context->>'causal_root_kind',context->>'causal_root_id',clock_timestamp());
  FOR item IN SELECT * FROM __forest_module_schema__.event_subscription_routes
    WHERE bot_id=context->>'bot_id' AND routing_generation_id=context->>'generation_id'
      AND topic=input->>'topic' AND contract=input->>'contract'
    ORDER BY subscriber_id,contract_major
  LOOP
    INSERT INTO __forest_module_schema__.event_deliveries(
      bot_id,delivery_id,event_id,subscriber_id,contract_major,topic,contract,
      event_schema_digest,owner_component_id,handler_service_id,handler_service_version,max_attempts)
    VALUES(context->>'bot_id','delivery:'||md5(event_key||chr(31)||item.subscriber_id||chr(31)||item.contract_major),
      event_key,item.subscriber_id,item.contract_major,item.topic,item.contract,
      item.event_schema_digest,item.owner_component_id,item.handler_service_id,
      item.handler_service_version,item.max_attempts);
    delivery_total:=delivery_total+1;
  END LOOP;
  RETURN __forest_module_schema__.domain_ok(jsonb_build_object(
    'event_id',event_key,'delivery_count',delivery_total,'replayed',false));
END;
$$;

CREATE FUNCTION component_job_schedule(context jsonb,input jsonb) RETURNS jsonb
LANGUAGE plpgsql VOLATILE SET search_path=pg_catalog AS $$
DECLARE route record; previous record; job_key text; input_hash text; directory_graph text;
BEGIN
  input_hash:=md5(jsonb_build_object(
    'owner_component_id',input->>'owner_component_id','handler_id',input->>'handler_id',
    'payload',input->'payload','delay_seconds',input->'delay_seconds')::text);
  job_key:='component-job:'||md5((context->>'bot_id')||chr(31)||(input->>'owner_component_id')||chr(31)||(context->>'operation_id'));
  PERFORM pg_advisory_xact_lock(hashtextextended(job_key,0));
  SELECT job_id,input_digest,state INTO previous FROM __forest_module_schema__.component_jobs
   WHERE bot_id=context->>'bot_id' AND owner_component_id=input->>'owner_component_id'
     AND operation_id=context->>'operation_id';
  IF FOUND THEN
    IF previous.input_digest<>input_hash THEN
      RETURN __forest_module_schema__.domain_error('COMPONENT_JOB_OPERATION_CONFLICT','ComponentJob operation input differs',409);
    END IF;
    RETURN __forest_module_schema__.domain_ok(jsonb_build_object(
      'job_id',previous.job_id,'state',previous.state,'replayed',true));
  END IF;
  SELECT r.*,d.graph_id INTO route FROM __forest_module_schema__.component_job_routes r
   JOIN __forest_module_schema__.routing_directories d USING(bot_id,routing_generation_id)
   WHERE r.bot_id=context->>'bot_id' AND r.routing_generation_id=context->>'generation_id'
     AND r.owner_component_id=input->>'owner_component_id'
     AND r.owner_generation_id=input->>'owner_generation_id'
     AND r.handler_id=input->>'handler_id';
  IF NOT FOUND THEN
    RETURN __forest_module_schema__.domain_error('COMPONENT_JOB_ROUTE_UNAVAILABLE','ComponentJob route is unavailable',409);
  END IF;
  directory_graph:=route.graph_id;
  INSERT INTO __forest_module_schema__.component_jobs(
    bot_id,job_id,operation_id,input_digest,owner_component_id,scheduled_by_generation_id,
    graph_id,handler_id,input_schema_digest,input_payload,causal_root_kind,causal_root_id,
    max_attempts,timeout_seconds,max_concurrency,handler_service_id,handler_service_version,available_at)
  VALUES(context->>'bot_id',job_key,context->>'operation_id',input_hash,
    input->>'owner_component_id',input->>'owner_generation_id',directory_graph,input->>'handler_id',
    route.input_schema_digest,input->'payload',context->>'causal_root_kind',context->>'causal_root_id',
    route.max_attempts,route.timeout_seconds,route.max_concurrency,route.handler_service_id,
    route.handler_service_version,clock_timestamp()+make_interval(secs=>(input->>'delay_seconds')::double precision));
  RETURN __forest_module_schema__.domain_ok(jsonb_build_object(
    'job_id',job_key,'state','WAITING','replayed',false));
END;
$$;

CREATE FUNCTION event_delivery_claim(context jsonb,input jsonb) RETURNS jsonb
LANGUAGE plpgsql VOLATILE SET search_path=pg_catalog AS $$
DECLARE selected record; attempt_no integer; attempt_key text; lease_s integer;
BEGIN
  lease_s:=(input->>'lease_seconds')::integer;
  UPDATE __forest_module_schema__.event_delivery_attempts a
    SET state='UNKNOWN',error_code='DELIVERY_LEASE_EXPIRED',completed_at=clock_timestamp()
   FROM __forest_module_schema__.event_deliveries d
   WHERE a.bot_id=d.bot_id AND a.attempt_id=d.active_attempt_id
     AND d.bot_id=context->>'bot_id' AND d.state='CLAIMED'
     AND d.lease_until<=clock_timestamp() AND a.state='DISPATCHING';
  UPDATE __forest_module_schema__.event_deliveries
    SET state='UNKNOWN',revision=revision+1,claim_token=NULL,routing_generation_id=NULL,
        target_generation_id=NULL,active_attempt_id=NULL,lease_until=NULL,
        error_code='DELIVERY_LEASE_EXPIRED',completed_at=clock_timestamp(),updated_at=clock_timestamp()
   WHERE bot_id=context->>'bot_id' AND state='CLAIMED' AND lease_until<=clock_timestamp();
  UPDATE __forest_module_schema__.event_deliveries d
    SET state='DEAD_LETTER',revision=d.revision+1,error_code='EVENT_ROUTE_UNAVAILABLE',
        completed_at=clock_timestamp(),updated_at=clock_timestamp()
   WHERE d.bot_id=context->>'bot_id' AND d.state IN ('QUEUED','RETRY_WAIT')
     AND NOT EXISTS(
       SELECT 1 FROM __forest_module_schema__.event_subscription_routes r
        WHERE r.bot_id=d.bot_id AND r.routing_generation_id=context->>'generation_id'
          AND r.subscriber_id=d.subscriber_id AND r.contract_major=d.contract_major
          AND r.topic=d.topic AND r.contract=d.contract
          AND r.event_schema_digest=d.event_schema_digest
          AND r.owner_component_id=d.owner_component_id
          AND r.handler_service_id=d.handler_service_id
          AND r.handler_service_version=d.handler_service_version);
  SELECT d.*,e.publisher_component_id,e.publisher_generation_id,e.payload,
         e.causal_root_kind,e.causal_root_id,r.owner_generation_id AS next_target_generation
    INTO selected
    FROM __forest_module_schema__.event_deliveries d
    JOIN __forest_module_schema__.component_events e
      ON e.bot_id=d.bot_id AND e.event_id=d.event_id
    JOIN __forest_module_schema__.event_subscription_routes r
      ON r.bot_id=d.bot_id AND r.routing_generation_id=context->>'generation_id'
     AND r.subscriber_id=d.subscriber_id AND r.contract_major=d.contract_major
     AND r.topic=d.topic AND r.contract=d.contract
     AND r.event_schema_digest=d.event_schema_digest
     AND r.owner_component_id=d.owner_component_id
     AND r.handler_service_id=d.handler_service_id
     AND r.handler_service_version=d.handler_service_version
   WHERE d.bot_id=context->>'bot_id' AND d.state IN ('QUEUED','RETRY_WAIT')
     AND d.available_at<=clock_timestamp()
     AND NOT EXISTS(
       SELECT 1 FROM __forest_module_schema__.event_deliveries active
        WHERE active.bot_id=d.bot_id AND active.subscriber_id=d.subscriber_id
          AND active.state='CLAIMED')
   ORDER BY d.available_at,d.updated_at,d.delivery_id
   FOR UPDATE OF d SKIP LOCKED LIMIT 1;
  IF NOT FOUND THEN RETURN __forest_module_schema__.domain_ok(NULL); END IF;
  attempt_no:=selected.attempt_count+1;
  attempt_key:='event-attempt:'||md5(selected.delivery_id||chr(31)||attempt_no::text);
  UPDATE __forest_module_schema__.event_deliveries
    SET state='CLAIMED',revision=revision+1,attempt_count=attempt_count+1,
        claim_token=input->>'claim_token',routing_generation_id=context->>'generation_id',
        target_generation_id=selected.next_target_generation,active_attempt_id=attempt_key,
        lease_until=clock_timestamp()+make_interval(secs=>lease_s),error_code=NULL,
        updated_at=clock_timestamp()
   WHERE bot_id=context->>'bot_id' AND delivery_id=selected.delivery_id;
  INSERT INTO __forest_module_schema__.event_delivery_attempts VALUES(
    context->>'bot_id',attempt_key,selected.delivery_id,attempt_no,input->>'claim_token',
    context->>'generation_id',selected.next_target_generation,'DISPATCHING',NULL,
    clock_timestamp(),NULL);
  RETURN __forest_module_schema__.domain_ok(jsonb_build_object(
    'protocol','forest.owned-work-claim/1','work_id',selected.delivery_id,
    'claim_token',input->>'claim_token','attempt',attempt_no,'operation_id',attempt_key,
    'causal_root_kind',selected.causal_root_kind,'causal_root_id',selected.causal_root_id,
    'payload',jsonb_build_object(
      'event_id',selected.event_id,'delivery_id',selected.delivery_id,
      'subscriber_id',selected.subscriber_id,'topic',selected.topic,'contract',selected.contract,
      'publisher_component_id',selected.publisher_component_id,
      'publisher_generation_id',selected.publisher_generation_id,'payload',selected.payload,
      'target_component_id',selected.owner_component_id,
      'target_generation_id',selected.next_target_generation,
      'target_service_id',selected.handler_service_id,
      'target_service_version',selected.handler_service_version,
      'attempt_id',attempt_key,'timeout_seconds',30)));
END;
$$;

CREATE FUNCTION event_delivery_settle(context jsonb,input jsonb) RETURNS jsonb
LANGUAGE plpgsql VOLATILE SET search_path=pg_catalog AS $$
DECLARE previous record; current_item record; final_state text; attempt_state text;
        v_error_code text; can_retry boolean; ack jsonb;
BEGIN
  ack:=jsonb_build_object('protocol','forest.owned-work-settled/1','work_id',input->>'work_id');
  SELECT input_payload,result_payload INTO previous FROM __forest_module_schema__.work_settlements
   WHERE claim_token=input->>'claim_token';
  IF FOUND THEN
    IF previous.input_payload<>input THEN
      RETURN __forest_module_schema__.domain_error('OWNED_WORK_SETTLEMENT_CONFLICT','Settlement input differs',409);
    END IF;
    RETURN __forest_module_schema__.domain_ok(previous.result_payload);
  END IF;
  SELECT * INTO current_item FROM __forest_module_schema__.event_deliveries
   WHERE bot_id=context->>'bot_id' AND delivery_id=input->>'work_id' AND state='CLAIMED'
     AND claim_token=input->>'claim_token' AND routing_generation_id=context->>'generation_id'
     AND lease_until>clock_timestamp() FOR UPDATE;
  IF NOT FOUND THEN
    RETURN __forest_module_schema__.domain_error('STALE_ROUTING_CLAIM','Event delivery claim is stale',409);
  END IF;
  IF input->'outcome'->>'status'='SUCCEEDED' THEN
    final_state:='SUCCEEDED'; attempt_state:='SUCCEEDED'; v_error_code:=NULL; can_retry:=false;
  ELSE
    v_error_code:=input->'outcome'->'error'->>'code';
    IF COALESCE((input->'outcome'->'error'->>'unknown')::boolean,false) THEN
      final_state:='UNKNOWN'; attempt_state:='UNKNOWN'; can_retry:=false;
    ELSE
      can_retry:=COALESCE((input->'outcome'->'error'->>'retryable')::boolean,false)
        AND current_item.attempt_count<current_item.max_attempts;
      final_state:=CASE WHEN can_retry THEN 'RETRY_WAIT' ELSE 'DEAD_LETTER' END;
      attempt_state:='FAILED';
    END IF;
  END IF;
  UPDATE __forest_module_schema__.event_delivery_attempts
    SET state=attempt_state,error_code=v_error_code,completed_at=clock_timestamp()
   WHERE bot_id=context->>'bot_id' AND attempt_id=current_item.active_attempt_id
     AND state='DISPATCHING';
  UPDATE __forest_module_schema__.event_deliveries
    SET state=final_state,revision=revision+1,claim_token=NULL,routing_generation_id=NULL,
        target_generation_id=NULL,active_attempt_id=NULL,lease_until=NULL,
        error_code=CASE WHEN can_retry THEN NULL ELSE v_error_code END,
        available_at=CASE WHEN can_retry THEN clock_timestamp()+interval '1 second' ELSE available_at END,
        completed_at=CASE WHEN final_state IN ('SUCCEEDED','DEAD_LETTER','UNKNOWN')
                          THEN clock_timestamp() ELSE NULL END,
        updated_at=clock_timestamp()
   WHERE bot_id=context->>'bot_id' AND delivery_id=current_item.delivery_id;
  INSERT INTO __forest_module_schema__.work_settlements VALUES(input->>'claim_token',input,ack);
  RETURN __forest_module_schema__.domain_ok(ack);
END;
$$;

CREATE FUNCTION component_job_claim(context jsonb,input jsonb) RETURNS jsonb
LANGUAGE plpgsql VOLATILE SET search_path=pg_catalog AS $$
DECLARE selected record; attempt_no integer; attempt_key text; lease_s integer;
BEGIN
  lease_s:=(input->>'lease_seconds')::integer;
  UPDATE __forest_module_schema__.component_job_attempts a
    SET state='UNKNOWN',error_code='JOB_LEASE_EXPIRED',completed_at=clock_timestamp()
   FROM __forest_module_schema__.component_jobs j
   WHERE a.bot_id=j.bot_id AND a.attempt_id=j.active_attempt_id
     AND j.bot_id=context->>'bot_id' AND j.state='CLAIMED'
     AND j.lease_until<=clock_timestamp() AND a.state='DISPATCHING';
  UPDATE __forest_module_schema__.component_jobs
    SET state='UNKNOWN',revision=revision+1,claim_token=NULL,routing_generation_id=NULL,
        target_generation_id=NULL,active_attempt_id=NULL,lease_until=NULL,
        error_code='JOB_LEASE_EXPIRED',completed_at=clock_timestamp(),updated_at=clock_timestamp()
   WHERE bot_id=context->>'bot_id' AND state='CLAIMED' AND lease_until<=clock_timestamp();
  UPDATE __forest_module_schema__.component_jobs j
    SET state='FAILED',revision=j.revision+1,error_code='COMPONENT_JOB_ROUTE_UNAVAILABLE',
        completed_at=clock_timestamp(),updated_at=clock_timestamp()
   WHERE j.bot_id=context->>'bot_id' AND j.state IN ('WAITING','RETRY_WAIT')
     AND NOT EXISTS(
       SELECT 1 FROM __forest_module_schema__.component_job_routes r
        WHERE r.bot_id=j.bot_id AND r.routing_generation_id=context->>'generation_id'
          AND r.owner_component_id=j.owner_component_id AND r.handler_id=j.handler_id
          AND r.input_schema_digest=j.input_schema_digest
          AND r.handler_service_id=j.handler_service_id
          AND r.handler_service_version=j.handler_service_version);
  SELECT j.*,r.owner_generation_id AS next_target_generation
    INTO selected FROM __forest_module_schema__.component_jobs j
    JOIN __forest_module_schema__.component_job_routes r
      ON r.bot_id=j.bot_id AND r.routing_generation_id=context->>'generation_id'
     AND r.owner_component_id=j.owner_component_id AND r.handler_id=j.handler_id
     AND r.input_schema_digest=j.input_schema_digest
     AND r.handler_service_id=j.handler_service_id
     AND r.handler_service_version=j.handler_service_version
   WHERE j.bot_id=context->>'bot_id' AND j.state IN ('WAITING','RETRY_WAIT')
     AND j.available_at<=clock_timestamp()
     AND (SELECT count(*) FROM __forest_module_schema__.component_jobs active
           WHERE active.bot_id=j.bot_id AND active.owner_component_id=j.owner_component_id
             AND active.handler_id=j.handler_id AND active.state='CLAIMED')<j.max_concurrency
   ORDER BY j.available_at,j.updated_at,j.job_id
   FOR UPDATE OF j SKIP LOCKED LIMIT 1;
  IF NOT FOUND THEN RETURN __forest_module_schema__.domain_ok(NULL); END IF;
  attempt_no:=selected.attempt_count+1;
  attempt_key:='component-job-attempt:'||md5(selected.job_id||chr(31)||attempt_no::text);
  UPDATE __forest_module_schema__.component_jobs
    SET state='CLAIMED',revision=revision+1,attempt_count=attempt_count+1,
        claim_token=input->>'claim_token',routing_generation_id=context->>'generation_id',
        target_generation_id=selected.next_target_generation,active_attempt_id=attempt_key,
        lease_until=clock_timestamp()+make_interval(secs=>lease_s),error_code=NULL,
        updated_at=clock_timestamp()
   WHERE bot_id=context->>'bot_id' AND job_id=selected.job_id;
  INSERT INTO __forest_module_schema__.component_job_attempts VALUES(
    context->>'bot_id',attempt_key,selected.job_id,attempt_no,input->>'claim_token',
    context->>'generation_id',selected.next_target_generation,'DISPATCHING',NULL,
    clock_timestamp(),NULL);
  RETURN __forest_module_schema__.domain_ok(jsonb_build_object(
    'protocol','forest.owned-work-claim/1','work_id',selected.job_id,
    'claim_token',input->>'claim_token','attempt',attempt_no,'operation_id',attempt_key,
    'causal_root_kind',selected.causal_root_kind,'causal_root_id',selected.causal_root_id,
    'payload',jsonb_build_object(
      'job_id',selected.job_id,'handler_id',selected.handler_id,
      'owner_component_id',selected.owner_component_id,
      'scheduled_by_generation_id',selected.scheduled_by_generation_id,
      'payload',selected.input_payload,'target_component_id',selected.owner_component_id,
      'target_generation_id',selected.next_target_generation,
      'target_service_id',selected.handler_service_id,
      'target_service_version',selected.handler_service_version,
      'attempt_id',attempt_key,'timeout_seconds',selected.timeout_seconds)));
END;
$$;

CREATE FUNCTION component_job_settle(context jsonb,input jsonb) RETURNS jsonb
LANGUAGE plpgsql VOLATILE SET search_path=pg_catalog AS $$
DECLARE previous record; current_item record; final_state text; attempt_state text;
        v_error_code text; can_retry boolean; ack jsonb; result_value jsonb;
BEGIN
  ack:=jsonb_build_object('protocol','forest.owned-work-settled/1','work_id',input->>'work_id');
  SELECT input_payload,result_payload INTO previous FROM __forest_module_schema__.work_settlements
   WHERE claim_token=input->>'claim_token';
  IF FOUND THEN
    IF previous.input_payload<>input THEN
      RETURN __forest_module_schema__.domain_error('OWNED_WORK_SETTLEMENT_CONFLICT','Settlement input differs',409);
    END IF;
    RETURN __forest_module_schema__.domain_ok(previous.result_payload);
  END IF;
  SELECT * INTO current_item FROM __forest_module_schema__.component_jobs
   WHERE bot_id=context->>'bot_id' AND job_id=input->>'work_id' AND state='CLAIMED'
     AND claim_token=input->>'claim_token' AND routing_generation_id=context->>'generation_id'
     AND lease_until>clock_timestamp() FOR UPDATE;
  IF NOT FOUND THEN
    RETURN __forest_module_schema__.domain_error('STALE_ROUTING_CLAIM','ComponentJob claim is stale',409);
  END IF;
  IF input->'outcome'->>'status'='SUCCEEDED' THEN
    final_state:='SUCCEEDED'; attempt_state:='SUCCEEDED'; v_error_code:=NULL;
    can_retry:=false; result_value:=input->'outcome'->'result';
  ELSE
    result_value:=NULL; v_error_code:=input->'outcome'->'error'->>'code';
    IF COALESCE((input->'outcome'->'error'->>'unknown')::boolean,false) THEN
      final_state:='UNKNOWN'; attempt_state:='UNKNOWN'; can_retry:=false;
    ELSE
      can_retry:=COALESCE((input->'outcome'->'error'->>'retryable')::boolean,false)
        AND current_item.attempt_count<current_item.max_attempts;
      final_state:=CASE WHEN can_retry THEN 'RETRY_WAIT' ELSE 'FAILED' END;
      attempt_state:='FAILED';
    END IF;
  END IF;
  UPDATE __forest_module_schema__.component_job_attempts
    SET state=attempt_state,error_code=v_error_code,completed_at=clock_timestamp()
   WHERE bot_id=context->>'bot_id' AND attempt_id=current_item.active_attempt_id
     AND state='DISPATCHING';
  UPDATE __forest_module_schema__.component_jobs
    SET state=final_state,revision=revision+1,claim_token=NULL,routing_generation_id=NULL,
        target_generation_id=NULL,active_attempt_id=NULL,lease_until=NULL,
        result_payload=result_value,error_code=CASE WHEN can_retry THEN NULL ELSE v_error_code END,
        available_at=CASE WHEN can_retry THEN clock_timestamp()+interval '1 second' ELSE available_at END,
        completed_at=CASE WHEN final_state IN ('SUCCEEDED','FAILED','UNKNOWN')
                          THEN clock_timestamp() ELSE NULL END,
        updated_at=clock_timestamp()
   WHERE bot_id=context->>'bot_id' AND job_id=current_item.job_id;
  INSERT INTO __forest_module_schema__.work_settlements VALUES(input->>'claim_token',input,ack);
  RETURN __forest_module_schema__.domain_ok(ack);
END;
$$;
