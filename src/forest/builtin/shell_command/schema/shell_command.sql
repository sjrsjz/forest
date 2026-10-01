CREATE TABLE shell_generation_snapshots (
    shell_generation_id text PRIMARY KEY,
    graph_id text NOT NULL,
    graph_revision bigint NOT NULL CHECK (graph_revision > 0),
    binding_ref text NOT NULL,
    policy_revision bigint NOT NULL CHECK (policy_revision > 0),
    refreshed_at timestamptz NOT NULL DEFAULT clock_timestamp()
);

CREATE TABLE shell_administrators (
    shell_generation_id text NOT NULL,
    principal_ref text NOT NULL,
    PRIMARY KEY (shell_generation_id, principal_ref),
    FOREIGN KEY (shell_generation_id) REFERENCES shell_generation_snapshots(shell_generation_id)
        ON DELETE CASCADE
);

CREATE TABLE shell_command_catalog (
    shell_generation_id text NOT NULL,
    command_id text NOT NULL,
    command_version integer NOT NULL CHECK (command_version > 0),
    graph_id text NOT NULL,
    graph_revision bigint NOT NULL CHECK (graph_revision > 0),
    argument_schema jsonb NOT NULL,
    argument_schema_digest text NOT NULL CHECK (argument_schema_digest ~ '^sha256:[a-f0-9]{64}$'),
    management_operation_id text NOT NULL,
    target_scope text NOT NULL CHECK (target_scope IN ('CONVERSATION','BINDING','BOT','DEPLOYMENT')),
    service_class text NOT NULL CHECK (service_class IN ('P0','P1')),
    required_capabilities jsonb NOT NULL,
    required_capabilities_digest text NOT NULL CHECK (required_capabilities_digest ~ '^sha256:[a-f0-9]{64}$'),
    output_policy text NOT NULL CHECK (output_policy IN ('SHORT','ARTIFACT','SILENT')),
    timeout_seconds integer NOT NULL CHECK (timeout_seconds BETWEEN 1 AND 900),
    owner_component_id text NOT NULL,
    target_generation_id text NOT NULL,
    target_service_id text NOT NULL,
    target_service_version integer NOT NULL CHECK (target_service_version > 0),
    PRIMARY KEY (shell_generation_id, command_id, command_version),
    FOREIGN KEY (shell_generation_id) REFERENCES shell_generation_snapshots(shell_generation_id)
        ON DELETE CASCADE
);

CREATE TABLE shell_requests (
    bot_id text NOT NULL,
    request_id text NOT NULL,
    admission_operation_id text NOT NULL,
    admission_input_digest text NOT NULL CHECK (admission_input_digest ~ '^sha256:[a-f0-9]{64}$'),
    shell_generation_id text NOT NULL,
    binding_ref text NOT NULL,
    binding_epoch bigint NOT NULL CHECK (binding_epoch > 0),
    connection_epoch bigint NOT NULL CHECK (connection_epoch > 0),
    client_incarnation text NOT NULL,
    adapter_generation_id text NOT NULL,
    executor_fence_digest text NOT NULL CHECK (executor_fence_digest ~ '^sha256:[a-f0-9]{64}$'),
    conversation_ref text NOT NULL,
    event_ref text NOT NULL,
    principal_ref text NOT NULL,
    command_id text NOT NULL,
    command_version integer NOT NULL CHECK (command_version > 0),
    arguments jsonb NOT NULL,
    policy_revision bigint NOT NULL CHECK (policy_revision > 0),
    service_class text CHECK (service_class IN ('P0','P1')),
    owner_component_id text,
    target_generation_id text,
    target_service_id text,
    target_service_version integer,
    timeout_seconds integer,
    state text NOT NULL CHECK (state IN (
        'QUEUED','REJECTION_PENDING','REJECTION_CLAIMED','REJECTED',
        'CLAIMED','EXECUTING','SUCCEEDED','FAILED','UNKNOWN'
    )),
    rejection_code text,
    rejection jsonb NOT NULL,
    claim_token text,
    attempt_count integer NOT NULL DEFAULT 0,
    lease_until timestamptz,
    authorization_ticket_id text,
    authorization_operation_id text,
    result jsonb,
    error_code text,
    received_at timestamptz NOT NULL,
    admission_deadline timestamptz NOT NULL,
    next_attempt_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    completed_at timestamptz,
    PRIMARY KEY (bot_id, request_id),
    UNIQUE (bot_id, admission_operation_id),
    UNIQUE (bot_id, event_ref)
);

CREATE INDEX shell_requests_queue_idx ON shell_requests(
    bot_id,shell_generation_id,state,service_class,created_at,request_id
);

CREATE TABLE shell_work_settlements (
    claim_token text PRIMARY KEY,
    input_payload jsonb NOT NULL,
    result_payload jsonb NOT NULL,
    settled_at timestamptz NOT NULL DEFAULT clock_timestamp()
);

CREATE TABLE shell_rejection_settlements (
    claim_token text PRIMARY KEY,
    input_payload jsonb NOT NULL,
    result_payload jsonb NOT NULL,
    settled_at timestamptz NOT NULL DEFAULT clock_timestamp()
);

CREATE FUNCTION shell_directory_synchronize(context jsonb, input jsonb)
RETURNS jsonb LANGUAGE plpgsql VOLATILE SET search_path=pg_catalog AS $$
DECLARE entry jsonb; principal jsonb; current_snapshot record;
        shell_generation text:=context->>'generation_id'; config jsonb:=input->'consumer_configuration';
BEGIN
  IF input->>'protocol' IS DISTINCT FROM 'forest.contribution-directory/1'
     OR input->>'kind' IS DISTINCT FROM 'shell-commands'
     OR jsonb_typeof(input->'items') IS DISTINCT FROM 'array'
     OR config->>'protocol' IS DISTINCT FROM 'forest.shell.command/2'
     OR jsonb_typeof(config->'administrator_principals') IS DISTINCT FROM 'array' THEN
    RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,'error',
      jsonb_build_object('code','SHELL_DIRECTORY_INVALID','message','Shell directory is invalid','status',422));
  END IF;
  PERFORM pg_advisory_xact_lock(hashtextextended(
    'forest.shell.catalog/'||(context->>'bot_id')||'/'||shell_generation,0));
  SELECT * INTO current_snapshot FROM __forest_module_schema__.shell_generation_snapshots
   WHERE shell_generation_id=shell_generation;
  IF FOUND THEN
    IF current_snapshot.graph_id IS DISTINCT FROM input->>'graph_id'
       OR current_snapshot.graph_revision IS DISTINCT FROM (input->>'graph_revision')::bigint
       OR current_snapshot.binding_ref IS DISTINCT FROM config->>'binding_ref' THEN
      RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,'error',
        jsonb_build_object('code','SHELL_DIRECTORY_OWNERSHIP_CONFLICT','message','Shell Generation snapshot differs','status',409));
    END IF;
    RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',true,'value',
      jsonb_build_object('graph_id',current_snapshot.graph_id,'graph_revision',current_snapshot.graph_revision,
        'item_count',(SELECT count(*) FROM __forest_module_schema__.shell_command_catalog WHERE shell_generation_id=shell_generation),
        'administrator_count',(SELECT count(*) FROM __forest_module_schema__.shell_administrators WHERE shell_generation_id=shell_generation)));
  END IF;
  INSERT INTO __forest_module_schema__.shell_generation_snapshots(
    shell_generation_id,graph_id,graph_revision,binding_ref,policy_revision)
  VALUES(shell_generation,input->>'graph_id',(input->>'graph_revision')::bigint,
    config->>'binding_ref',(input->>'graph_revision')::bigint);
  FOR principal IN SELECT value FROM jsonb_array_elements(config->'administrator_principals') LOOP
    INSERT INTO __forest_module_schema__.shell_administrators(shell_generation_id,principal_ref)
    VALUES(shell_generation,principal #>> '{}');
  END LOOP;
  FOR entry IN SELECT value FROM jsonb_array_elements(input->'items') LOOP
    INSERT INTO __forest_module_schema__.shell_command_catalog(
      shell_generation_id,command_id,command_version,graph_id,graph_revision,
      argument_schema,argument_schema_digest,management_operation_id,target_scope,
      service_class,required_capabilities,required_capabilities_digest,output_policy,
      timeout_seconds,owner_component_id,target_generation_id,target_service_id,target_service_version)
    VALUES(shell_generation,entry->>'command_id',(entry->>'command_version')::integer,
      input->>'graph_id',(input->>'graph_revision')::bigint,entry->'argument_schema',
      entry->>'argument_schema_digest',entry->>'management_operation_id',entry->>'target_scope',
      entry->>'service_class',entry->'required_capabilities',entry->>'required_capabilities_digest',
      entry->>'output_policy',(entry->>'timeout_seconds')::integer,entry->>'owner_component_id',
      entry->>'target_generation_id',entry->>'service_id',(entry->>'service_version')::integer);
  END LOOP;
  RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',true,'value',
    jsonb_build_object('graph_id',input->>'graph_id','graph_revision',(input->>'graph_revision')::bigint,
      'item_count',jsonb_array_length(input->'items'),
      'administrator_count',jsonb_array_length(config->'administrator_principals')));
END;
$$;

CREATE FUNCTION shell_directory_cleanup(context jsonb, input jsonb)
RETURNS jsonb LANGUAGE plpgsql VOLATILE SET search_path=pg_catalog AS $$
DECLARE snapshot record; shell_generation text:=context->>'generation_id'; released boolean:=false;
BEGIN
  SELECT * INTO snapshot FROM __forest_module_schema__.shell_generation_snapshots
   WHERE shell_generation_id=shell_generation FOR UPDATE;
  IF FOUND AND (snapshot.graph_id IS DISTINCT FROM input->>'graph_id'
      OR snapshot.graph_revision IS DISTINCT FROM (input->>'graph_revision')::bigint) THEN
    RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,'error',
      jsonb_build_object('code','SHELL_DIRECTORY_OWNERSHIP_CONFLICT','message','Shell directory belongs to another graph','status',409));
  END IF;
  released:=FOUND;
  DELETE FROM __forest_module_schema__.shell_generation_snapshots
   WHERE shell_generation_id=shell_generation;
  RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',true,'value',
    jsonb_build_object('released',released));
END;
$$;

CREATE FUNCTION shell_admit_ingress(context jsonb, envelope jsonb)
RETURNS jsonb LANGUAGE plpgsql VOLATILE SET search_path=pg_catalog AS $$
DECLARE input jsonb:=envelope->'input'; snapshot record; route record; existing record;
        allowed boolean; next_state text; rejection_code text; result jsonb;
BEGIN
  IF envelope->>'protocol' IS DISTINCT FROM 'forest.uow-step/1'
     OR input->>'protocol' IS DISTINCT FROM 'forest.shell-admission/2'
     OR input->>'shell_generation_id' IS DISTINCT FROM context->>'generation_id' THEN
    RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,'error',
      jsonb_build_object('code','SHELL_ADMISSION_INVALID','message','Shell admission is invalid','status',422));
  END IF;
  PERFORM pg_advisory_xact_lock(hashtextextended(
    'forest.shell.request/'||(context->>'bot_id')||'/'||(input->>'request_id'),0));
  SELECT * INTO existing FROM __forest_module_schema__.shell_requests
   WHERE bot_id=context->>'bot_id' AND request_id=input->>'request_id';
  IF FOUND THEN
    IF existing.admission_input_digest IS DISTINCT FROM context->>'input_digest' THEN
      RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,'error',
        jsonb_build_object('code','SHELL_REQUEST_CONFLICT','message','Shell request replay differs','status',409));
    END IF;
    RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',true,'value',
      jsonb_build_object('request_id',existing.request_id,'state',existing.state,
        'shell_generation_id',existing.shell_generation_id,'policy_revision',existing.policy_revision,
        'rejection_code',existing.rejection_code,'rejection',existing.rejection,
        'service_class',existing.service_class));
  END IF;
  SELECT * INTO snapshot FROM __forest_module_schema__.shell_generation_snapshots
   WHERE shell_generation_id=context->>'generation_id';
  IF NOT FOUND THEN
    RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,'error',
      jsonb_build_object('code','SHELL_DIRECTORY_UNAVAILABLE','message','Shell Generation is not materialized','status',503));
  END IF;
  SELECT * INTO route FROM __forest_module_schema__.shell_command_catalog
   WHERE shell_generation_id=context->>'generation_id' AND command_id=input->>'command_id'
     AND command_version=(input->>'command_version')::integer;
  SELECT EXISTS(SELECT 1 FROM __forest_module_schema__.shell_administrators
    WHERE shell_generation_id=context->>'generation_id' AND principal_ref=input->>'principal_ref') INTO allowed;
  rejection_code:=CASE
    WHEN input->>'binding_ref' IS DISTINCT FROM snapshot.binding_ref THEN 'SHELL_BINDING_DENIED'
    WHEN (input->>'admission_deadline')::timestamptz <= clock_timestamp() THEN 'SHELL_ADMISSION_EXPIRED'
    WHEN NOT allowed THEN 'SHELL_PRINCIPAL_DENIED'
    WHEN route IS NULL THEN 'SHELL_COMMAND_NOT_FOUND'
    ELSE NULL END;
  next_state:=CASE WHEN rejection_code IS NULL THEN 'QUEUED' ELSE 'REJECTION_PENDING' END;
  INSERT INTO __forest_module_schema__.shell_requests(
    bot_id,request_id,admission_operation_id,admission_input_digest,shell_generation_id,
    binding_ref,binding_epoch,connection_epoch,client_incarnation,adapter_generation_id,
    executor_fence_digest,conversation_ref,event_ref,principal_ref,command_id,command_version,
    arguments,policy_revision,service_class,owner_component_id,target_generation_id,
    target_service_id,target_service_version,timeout_seconds,state,rejection_code,rejection,
    received_at,admission_deadline,completed_at)
  VALUES(context->>'bot_id',input->>'request_id',context->>'operation_id',context->>'input_digest',
    context->>'generation_id',input->>'binding_ref',(input->>'binding_epoch')::bigint,
    (input->>'connection_epoch')::bigint,input->>'client_incarnation',input->>'adapter_generation_id',
    input->>'executor_fence_digest',input->>'conversation_ref',input->>'event_ref',input->>'principal_ref',
    input->>'command_id',(input->>'command_version')::integer,input->'arguments',snapshot.policy_revision,
    route.service_class,route.owner_component_id,route.target_generation_id,route.target_service_id,
    route.target_service_version,route.timeout_seconds,next_state,rejection_code,input->'rejection',
    (input->>'received_at')::timestamptz,(input->>'admission_deadline')::timestamptz,
    NULL);
  result:=jsonb_build_object('request_id',input->>'request_id','state',next_state,
    'shell_generation_id',context->>'generation_id','policy_revision',snapshot.policy_revision,
    'rejection_code',rejection_code,'rejection',input->'rejection','service_class',route.service_class);
  RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',true,'value',result);
END;
$$;

CREATE FUNCTION shell_authorize_claim(context jsonb, envelope jsonb)
RETURNS jsonb LANGUAGE plpgsql VOLATILE SET search_path=pg_catalog AS $$
DECLARE input jsonb:=envelope->'input'; item __forest_module_schema__.shell_requests%ROWTYPE;
BEGIN
  IF envelope->>'protocol' IS DISTINCT FROM 'forest.uow-step/1'
     OR envelope->'prior'->'consume'->>'protocol' IS DISTINCT FROM 'forest.authorization-consumed/1' THEN
    RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,'error',
      jsonb_build_object('code','SHELL_AUTHORIZATION_INVALID','message','Shell authorization evidence is invalid','status',403));
  END IF;
  SELECT * INTO item FROM __forest_module_schema__.shell_requests
   WHERE bot_id=context->>'bot_id' AND request_id=input->>'request_id'
     AND shell_generation_id=context->>'generation_id' AND state='CLAIMED'
     AND claim_token=input->>'claim_token' AND lease_until>clock_timestamp() FOR UPDATE;
  IF NOT FOUND THEN
    RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,'error',
      jsonb_build_object('code','STALE_SHELL_CLAIM','message','Shell claim is no longer owned','status',409));
  END IF;
  IF NOT EXISTS(SELECT 1 FROM __forest_module_schema__.shell_administrators
      WHERE shell_generation_id=context->>'generation_id' AND principal_ref=item.principal_ref) THEN
    RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,'error',
      jsonb_build_object('code','SHELL_PRINCIPAL_REVOKED','message','Shell principal is no longer authorized','status',403));
  END IF;
  UPDATE __forest_module_schema__.shell_requests SET state='EXECUTING',
    authorization_ticket_id=input->>'ticket_id',authorization_operation_id=input->>'authorization_operation_id',
    updated_at=clock_timestamp() WHERE bot_id=item.bot_id AND request_id=item.request_id;
  RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',true,'value',
    jsonb_build_object('request_id',item.request_id,'state','EXECUTING'));
END;
$$;

CREATE FUNCTION shell_verify_management_claim(context jsonb, input jsonb)
RETURNS jsonb LANGUAGE plpgsql VOLATILE SET search_path=pg_catalog AS $$
DECLARE item __forest_module_schema__.shell_requests%ROWTYPE;
        route __forest_module_schema__.shell_command_catalog%ROWTYPE;
BEGIN
  IF input->>'protocol' IS DISTINCT FROM 'forest.shell-management-claim/1'
     OR input->>'shell_generation_id' IS DISTINCT FROM context->>'generation_id' THEN
    RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,'error',
      jsonb_build_object('code','SHELL_MANAGEMENT_CLAIM_INVALID','message','Shell management claim is invalid','status',403));
  END IF;
  SELECT * INTO item FROM __forest_module_schema__.shell_requests
   WHERE bot_id=context->>'bot_id' AND request_id=input->>'request_id'
   FOR SHARE;
  IF NOT FOUND OR item.state!='EXECUTING' OR item.claim_token IS DISTINCT FROM input->>'claim_token'
     OR item.lease_until IS NULL OR item.lease_until<=clock_timestamp()
     OR item.authorization_ticket_id IS NULL OR item.authorization_operation_id IS NULL
     OR item.shell_generation_id IS DISTINCT FROM input->>'shell_generation_id'
     OR item.principal_ref IS DISTINCT FROM input->>'principal_ref'
     OR item.conversation_ref IS DISTINCT FROM input->>'conversation_ref'
     OR item.command_id IS DISTINCT FROM input->>'command_id'
     OR item.command_version IS DISTINCT FROM (input->>'command_version')::integer
     OR item.arguments IS DISTINCT FROM input->'arguments'
     OR item.owner_component_id IS DISTINCT FROM input->>'target_component_id'
     OR item.target_generation_id IS DISTINCT FROM input->>'target_generation_id'
     OR item.target_service_id IS DISTINCT FROM input->>'target_service_id'
     OR item.target_service_version IS DISTINCT FROM (input->>'target_service_version')::integer THEN
    RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,'error',
      jsonb_build_object('code','SHELL_MANAGEMENT_CLAIM_STALE','message','Shell management claim is stale','status',409));
  END IF;
  SELECT * INTO route FROM __forest_module_schema__.shell_command_catalog
   WHERE shell_generation_id=item.shell_generation_id AND command_id=item.command_id
     AND command_version=item.command_version;
  IF NOT FOUND OR route.management_operation_id IS DISTINCT FROM input->>'management_operation_id'
     OR route.owner_component_id IS DISTINCT FROM item.owner_component_id
     OR route.target_generation_id IS DISTINCT FROM item.target_generation_id
     OR route.target_service_id IS DISTINCT FROM item.target_service_id
     OR route.target_service_version IS DISTINCT FROM item.target_service_version
     OR NOT EXISTS(SELECT 1 FROM __forest_module_schema__.shell_administrators a
       WHERE a.shell_generation_id=item.shell_generation_id AND a.principal_ref=item.principal_ref) THEN
    RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,'error',
      jsonb_build_object('code','SHELL_MANAGEMENT_AUTHORITY_REVOKED','message','Shell management authority is unavailable','status',403));
  END IF;
  RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',true,'value',
    jsonb_build_object('protocol','forest.shell-management-claim-verified/1',
      'request_id',item.request_id,'principal_ref',item.principal_ref,
      'ticket_id',item.authorization_ticket_id));
END;
$$;

CREATE FUNCTION shell_complete_claim(context jsonb, envelope jsonb)
RETURNS jsonb LANGUAGE plpgsql VOLATILE SET search_path=pg_catalog AS $$
DECLARE input jsonb:=envelope->'input'; item __forest_module_schema__.shell_requests%ROWTYPE;
BEGIN
  SELECT * INTO item FROM __forest_module_schema__.shell_requests
   WHERE bot_id=context->>'bot_id' AND request_id=input->>'request_id'
     AND shell_generation_id=context->>'generation_id' AND state IN ('CLAIMED','EXECUTING')
     AND claim_token=input->>'claim_token' AND lease_until>clock_timestamp() FOR UPDATE;
  IF NOT FOUND THEN
    RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,'error',
      jsonb_build_object('code','STALE_SHELL_CLAIM','message','Shell claim is no longer owned','status',409));
  END IF;
  UPDATE __forest_module_schema__.shell_requests SET state=input->>'resulting_state',
    result=input->'result',error_code=input->>'error_code',updated_at=clock_timestamp(),
    completed_at=clock_timestamp() WHERE bot_id=item.bot_id AND request_id=item.request_id;
  RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',true,'value',
    jsonb_build_object('request_id',item.request_id,'state',input->>'resulting_state'));
END;
$$;

CREATE FUNCTION shell_claim_work(context jsonb, input jsonb)
RETURNS jsonb LANGUAGE plpgsql VOLATILE SET search_path=pg_catalog AS $$
DECLARE item __forest_module_schema__.shell_requests%ROWTYPE;
        token text:=input->>'claim_token'; lease_seconds integer:=(input->>'lease_seconds')::integer;
BEGIN
  UPDATE __forest_module_schema__.shell_requests SET state='UNKNOWN',error_code='SHELL_RESULT_UNKNOWN_AFTER_LEASE',
    completed_at=clock_timestamp(),updated_at=clock_timestamp()
   WHERE bot_id=context->>'bot_id' AND shell_generation_id=context->>'generation_id'
     AND state IN ('CLAIMED','EXECUTING') AND lease_until<=clock_timestamp();
  SELECT * INTO item FROM __forest_module_schema__.shell_requests
   WHERE bot_id=context->>'bot_id' AND shell_generation_id=context->>'generation_id'
     AND state='QUEUED' ORDER BY CASE service_class WHEN 'P0' THEN 0 ELSE 1 END,
       created_at,request_id FOR UPDATE SKIP LOCKED LIMIT 1;
  IF NOT FOUND THEN
    RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',true,'value',NULL);
  END IF;
  UPDATE __forest_module_schema__.shell_requests SET state='CLAIMED',claim_token=token,
    attempt_count=attempt_count+1,lease_until=clock_timestamp()+lease_seconds*interval '1 second',
    updated_at=clock_timestamp() WHERE bot_id=item.bot_id AND request_id=item.request_id RETURNING * INTO item;
  RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',true,'value',
    jsonb_build_object('protocol','forest.owned-work-claim/1','work_id',item.request_id,
      'claim_token',item.claim_token,'attempt',item.attempt_count,
      'operation_id','shell.execute:'||item.request_id||':'||item.attempt_count,
      'causal_root_kind','shell.request','causal_root_id',item.request_id,
      'payload',jsonb_build_object('request_id',item.request_id,'claim_token',item.claim_token,
        'binding_ref',item.binding_ref,'binding_epoch',item.binding_epoch,'connection_epoch',item.connection_epoch,
        'client_incarnation',item.client_incarnation,'adapter_generation_id',item.adapter_generation_id,
        'executor_fence_digest',item.executor_fence_digest,'conversation_ref',item.conversation_ref,
        'principal_ref',item.principal_ref,'command_id',item.command_id,'command_version',item.command_version,
        'arguments',item.arguments,'target_component_id',item.owner_component_id,
        'target_generation_id',item.target_generation_id,'target_service_id',item.target_service_id,
        'target_service_version',item.target_service_version,'timeout_seconds',item.timeout_seconds)));
END;
$$;

CREATE FUNCTION shell_complete_rejection(context jsonb, envelope jsonb)
RETURNS jsonb LANGUAGE plpgsql VOLATILE SET search_path=pg_catalog AS $$
DECLARE input jsonb:=envelope->'input'; item __forest_module_schema__.shell_requests%ROWTYPE;
BEGIN
  IF envelope->>'protocol' IS DISTINCT FROM 'forest.uow-step/1'
     OR envelope#>>'{caller,component_id}' IS DISTINCT FROM 'shell.command' THEN
    RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,'error',
      jsonb_build_object('code','SHELL_REJECTION_INPUT_INVALID','message','Shell rejection completion is invalid','status',422));
  END IF;
  SELECT * INTO item FROM __forest_module_schema__.shell_requests
   WHERE bot_id=context->>'bot_id' AND request_id=input->>'request_id'
     AND shell_generation_id=context->>'generation_id' AND state='REJECTION_CLAIMED'
     AND claim_token=input->>'claim_token' AND lease_until>clock_timestamp() FOR UPDATE;
  IF NOT FOUND THEN
    RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,'error',
      jsonb_build_object('code','STALE_SHELL_REJECTION_CLAIM','message','Shell rejection claim is no longer owned','status',409));
  END IF;
  UPDATE __forest_module_schema__.shell_requests SET state='REJECTED',result=input->'result',
    claim_token=NULL,lease_until=NULL,updated_at=clock_timestamp(),completed_at=clock_timestamp()
   WHERE bot_id=item.bot_id AND request_id=item.request_id;
  RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',true,'value',
    jsonb_build_object('request_id',item.request_id,'state','REJECTED'));
END;
$$;

CREATE FUNCTION shell_claim_rejection(context jsonb, input jsonb)
RETURNS jsonb LANGUAGE plpgsql VOLATILE SET search_path=pg_catalog AS $$
DECLARE item __forest_module_schema__.shell_requests%ROWTYPE;
        token text:=input->>'claim_token'; lease_seconds integer:=(input->>'lease_seconds')::integer;
BEGIN
  UPDATE __forest_module_schema__.shell_requests SET state='REJECTION_PENDING',claim_token=NULL,
    lease_until=NULL,next_attempt_at=clock_timestamp(),updated_at=clock_timestamp()
   WHERE bot_id=context->>'bot_id' AND shell_generation_id=context->>'generation_id'
     AND state='REJECTION_CLAIMED' AND lease_until<=clock_timestamp();
  SELECT * INTO item FROM __forest_module_schema__.shell_requests
   WHERE bot_id=context->>'bot_id' AND shell_generation_id=context->>'generation_id'
     AND state='REJECTION_PENDING' AND next_attempt_at<=clock_timestamp()
   ORDER BY next_attempt_at,created_at,request_id FOR UPDATE SKIP LOCKED LIMIT 1;
  IF NOT FOUND THEN
    RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',true,'value',NULL);
  END IF;
  UPDATE __forest_module_schema__.shell_requests SET state='REJECTION_CLAIMED',claim_token=token,
    attempt_count=attempt_count+1,lease_until=clock_timestamp()+lease_seconds*interval '1 second',
    updated_at=clock_timestamp() WHERE bot_id=item.bot_id AND request_id=item.request_id RETURNING * INTO item;
  RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',true,'value',
    jsonb_build_object('protocol','forest.owned-work-claim/1','work_id',item.request_id,
      'claim_token',item.claim_token,'attempt',item.attempt_count,
      'operation_id','shell.reject:'||item.request_id||':'||item.attempt_count,
      'causal_root_kind','shell.request','causal_root_id',item.request_id,
      'payload',jsonb_build_object('request_id',item.request_id,'claim_token',item.claim_token,
        'binding_ref',item.binding_ref,'conversation_ref',item.conversation_ref,
        'principal_ref',item.principal_ref,'command_id',item.command_id,
        'rejection_code',item.rejection_code,'rejection',item.rejection)));
END;
$$;

CREATE FUNCTION shell_settle_rejection(context jsonb, input jsonb)
RETURNS jsonb LANGUAGE plpgsql VOLATILE SET search_path=pg_catalog AS $$
DECLARE existing record; item __forest_module_schema__.shell_requests%ROWTYPE; result jsonb;
BEGIN
  SELECT * INTO existing FROM __forest_module_schema__.shell_rejection_settlements
   WHERE claim_token=input->>'claim_token';
  IF FOUND THEN
    IF existing.input_payload=input THEN RETURN existing.result_payload; END IF;
    RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,'error',
      jsonb_build_object('code','SHELL_REJECTION_SETTLEMENT_CONFLICT','message','Shell rejection claim has another settlement','status',409));
  END IF;
  SELECT * INTO item FROM __forest_module_schema__.shell_requests
   WHERE bot_id=context->>'bot_id' AND request_id=input->>'work_id'
     AND shell_generation_id=context->>'generation_id' FOR UPDATE;
  IF NOT FOUND OR (item.state='REJECTION_CLAIMED' AND item.claim_token IS DISTINCT FROM input->>'claim_token') THEN
    RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,'error',
      jsonb_build_object('code','STALE_SHELL_REJECTION_CLAIM','message','Shell rejection claim is no longer owned','status',409));
  END IF;
  IF item.state='REJECTION_CLAIMED' THEN
    UPDATE __forest_module_schema__.shell_requests SET state='REJECTION_PENDING',claim_token=NULL,
      lease_until=NULL,next_attempt_at=clock_timestamp()+LEAST(60,item.attempt_count*2)*interval '1 second',
      error_code=COALESCE(input->'outcome'->'error'->>'code','SHELL_REJECTION_DELIVERY_FAILED'),
      updated_at=clock_timestamp() WHERE bot_id=item.bot_id AND request_id=item.request_id;
  ELSIF item.state<>'REJECTED' THEN
    RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,'error',
      jsonb_build_object('code','SHELL_REJECTION_STATE_CONFLICT','message','Shell rejection has an invalid state','status',409));
  END IF;
  result:=jsonb_build_object('protocol','forest.domain-result/1','ok',true,'value',
    jsonb_build_object('protocol','forest.owned-work-settled/1','work_id',item.request_id));
  INSERT INTO __forest_module_schema__.shell_rejection_settlements(claim_token,input_payload,result_payload)
    VALUES(input->>'claim_token',input,result);
  RETURN result;
END;
$$;

CREATE FUNCTION shell_settle_work(context jsonb, input jsonb)
RETURNS jsonb LANGUAGE plpgsql VOLATILE SET search_path=pg_catalog AS $$
DECLARE existing record; item __forest_module_schema__.shell_requests%ROWTYPE; result jsonb;
BEGIN
  SELECT * INTO existing FROM __forest_module_schema__.shell_work_settlements
   WHERE claim_token=input->>'claim_token';
  IF FOUND THEN
    IF existing.input_payload=input THEN RETURN existing.result_payload; END IF;
    RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,'error',
      jsonb_build_object('code','SHELL_SETTLEMENT_CONFLICT','message','Shell claim has another settlement','status',409));
  END IF;
  SELECT * INTO item FROM __forest_module_schema__.shell_requests
   WHERE bot_id=context->>'bot_id' AND request_id=input->>'work_id'
     AND shell_generation_id=context->>'generation_id' AND claim_token=input->>'claim_token' FOR UPDATE;
  IF NOT FOUND THEN
    RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,'error',
      jsonb_build_object('code','STALE_SHELL_CLAIM','message','Shell claim is no longer owned','status',409));
  END IF;
  IF item.state IN ('CLAIMED','EXECUTING') THEN
    UPDATE __forest_module_schema__.shell_requests SET state='UNKNOWN',
      error_code=COALESCE(input->'outcome'->'error'->>'code','SHELL_EXECUTION_UNKNOWN'),
      completed_at=clock_timestamp(),updated_at=clock_timestamp()
     WHERE bot_id=item.bot_id AND request_id=item.request_id;
  END IF;
  result:=jsonb_build_object('protocol','forest.domain-result/1','ok',true,'value',
    jsonb_build_object('protocol','forest.owned-work-settled/1','work_id',item.request_id));
  INSERT INTO __forest_module_schema__.shell_work_settlements(claim_token,input_payload,result_payload)
    VALUES(input->>'claim_token',input,result);
  RETURN result;
END;
$$;
