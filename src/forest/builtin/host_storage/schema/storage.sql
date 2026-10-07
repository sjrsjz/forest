CREATE TABLE physical_generations (
    resource_id text PRIMARY KEY CHECK (resource_id ~ '^resource/[a-f0-9-]{36}$'),
    host_id text NOT NULL CHECK (host_id ~ '^host/[a-f0-9]{32}$'),
    operation_id text NOT NULL UNIQUE CHECK (octet_length(operation_id) BETWEEN 1 AND 256),
    input_digest text NOT NULL CHECK (input_digest ~ '^sha256:[a-f0-9]{64}$'),
    deployment_id text NOT NULL CHECK (octet_length(deployment_id) BETWEEN 1 AND 128),
    bot_id text NOT NULL CHECK (octet_length(bot_id) BETWEEN 1 AND 128),
    storage_generation bigint NOT NULL CHECK (storage_generation > 0),
    kind text NOT NULL CHECK (kind = 'POSTGRES_DATABASE'),
    physical_name text NOT NULL UNIQUE CHECK (physical_name ~ '^forest_bot_[a-f0-9]{24}$'),
    role_name text NOT NULL UNIQUE CHECK (role_name ~ '^forest_role_[a-f0-9]{24}$'),
    owner_marker text NOT NULL UNIQUE CHECK (owner_marker ~ '^marker/[a-f0-9]{64}$'),
    state text NOT NULL CHECK (state IN ('RESERVED','ACTIVE','RETIRED','DELETED')),
    revision bigint NOT NULL DEFAULT 1 CHECK (revision > 0),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (host_id,kind,deployment_id,bot_id,storage_generation)
);

CREATE INDEX physical_generations_page
    ON physical_generations (host_id,kind,state,resource_id);
CREATE INDEX physical_generations_owner_page
    ON physical_generations (host_id,deployment_id,bot_id,resource_id);

CREATE TABLE realization_operations (
    operation_id text PRIMARY KEY CHECK (octet_length(operation_id) BETWEEN 1 AND 256),
    resource_id text NOT NULL REFERENCES physical_generations(resource_id),
    input_digest text NOT NULL CHECK (input_digest ~ '^sha256:[a-f0-9]{64}$'),
    result_payload jsonb NOT NULL,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp()
);

CREATE TABLE materialization_work (
    resource_id text PRIMARY KEY REFERENCES physical_generations(resource_id),
    request_operation_id text NOT NULL UNIQUE CHECK (octet_length(request_operation_id) BETWEEN 1 AND 256),
    request_digest text NOT NULL CHECK (request_digest ~ '^sha256:[a-f0-9]{64}$'),
    credential_ref text NOT NULL CHECK (octet_length(credential_ref) BETWEEN 10 AND 512),
    state text NOT NULL DEFAULT 'PENDING'
        CHECK (state IN ('PENDING','CLAIMED','SUCCEEDED','FAILED')),
    claim_token text UNIQUE CHECK (claim_token IS NULL OR octet_length(claim_token) BETWEEN 1 AND 256),
    attempt_count integer NOT NULL DEFAULT 0 CHECK (attempt_count >= 0),
    lease_deadline timestamptz,
    next_attempt_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    last_error_code text,
    result_payload jsonb,
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    CHECK ((state='CLAIMED')=(claim_token IS NOT NULL AND lease_deadline IS NOT NULL))
);
CREATE INDEX materialization_work_pending_idx
    ON materialization_work(state,next_attempt_at,resource_id) WHERE state='PENDING';
CREATE INDEX materialization_work_expired_idx
    ON materialization_work(lease_deadline,resource_id) WHERE state='CLAIMED';

CREATE TABLE materialization_settlements (
    claim_token text PRIMARY KEY CHECK (octet_length(claim_token) BETWEEN 1 AND 256),
    resource_id text NOT NULL REFERENCES physical_generations(resource_id),
    input_payload jsonb NOT NULL,
    result_payload jsonb NOT NULL,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp()
);

CREATE TABLE resource_reference_pins (
    resource_id text PRIMARY KEY REFERENCES physical_generations(resource_id),
    deployment_id text NOT NULL CHECK (octet_length(deployment_id) BETWEEN 1 AND 128),
    operation_id text NOT NULL UNIQUE CHECK (octet_length(operation_id) BETWEEN 1 AND 256),
    input_digest text NOT NULL CHECK (input_digest ~ '^sha256:[a-f0-9]{64}$'),
    profile_ref text NOT NULL CHECK (octet_length(profile_ref) BETWEEN 1 AND 512),
    resource_revision bigint NOT NULL CHECK (resource_revision >= 2),
    result_payload jsonb NOT NULL,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (deployment_id,profile_ref)
);

CREATE FUNCTION host_storage_reserve(context jsonb, input jsonb)
RETURNS jsonb LANGUAGE plpgsql VOLATILE SET search_path = pg_catalog AS $$
DECLARE
    requested_operation text := context->>'operation_id';
    requested_host text := context->>'scope_id';
    requested_deployment text := input->>'deployment_id';
    requested_bot text := input->>'bot_id';
    requested_generation bigint := (input->>'storage_generation')::bigint;
    name_digest text;
    item __forest_module_schema__.physical_generations%ROWTYPE;
BEGIN
    IF context->>'scope_kind' IS DISTINCT FROM 'host'
       OR context->>'component_id' IS DISTINCT FROM 'host.storage'
       OR context->>'port_kind' IS DISTINCT FROM 'command'
       OR context->>'port_name' IS DISTINCT FROM 'host.storage.reserve-record'
       OR requested_host !~ '^host/[a-f0-9]{32}$'
       OR requested_operation IS NULL OR octet_length(requested_operation) NOT BETWEEN 1 AND 256
       OR context->>'input_digest' !~ '^sha256:[a-f0-9]{64}$'
       OR requested_deployment IS NULL OR octet_length(requested_deployment) NOT BETWEEN 1 AND 128
       OR requested_bot IS NULL OR octet_length(requested_bot) NOT BETWEEN 1 AND 128
       OR requested_generation IS NULL OR requested_generation < 1 THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object('code','HOST_STORAGE_RESERVE_INVALID',
                'message','Host storage reservation is invalid','status',422));
    END IF;
    PERFORM pg_advisory_xact_lock(hashtextextended(
        'host.storage.operation/'||requested_host||'/'||requested_operation,0));
    SELECT * INTO item FROM __forest_module_schema__.physical_generations
        WHERE operation_id=requested_operation FOR UPDATE;
    IF FOUND THEN
        IF item.host_id<>requested_host OR item.input_digest<>context->>'input_digest' THEN
            RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
                'error',jsonb_build_object('code','HOST_STORAGE_OPERATION_CONFLICT',
                    'message','Reservation operation input differs','status',409));
        END IF;
    ELSE
        PERFORM pg_advisory_xact_lock(hashtextextended(
            'host.storage.resource/'||requested_host||'/'||requested_deployment||'/'||
            requested_bot||'/'||requested_generation::text,0));
        SELECT * INTO item FROM __forest_module_schema__.physical_generations
        WHERE host_id=requested_host AND kind='POSTGRES_DATABASE'
          AND deployment_id=requested_deployment AND bot_id=requested_bot
          AND storage_generation=requested_generation FOR UPDATE;
        IF FOUND THEN
            RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
                'error',jsonb_build_object('code','HOST_STORAGE_RESOURCE_CONFLICT',
                    'message','Storage generation already has another reservation','status',409));
        END IF;
        name_digest := encode(sha256(convert_to(
            requested_host||'/'||requested_deployment||'/'||requested_bot||'/'||
            requested_generation::text,'UTF8')),'hex');
        INSERT INTO __forest_module_schema__.physical_generations(
            resource_id,host_id,operation_id,input_digest,deployment_id,bot_id,
            storage_generation,kind,physical_name,role_name,owner_marker,state)
        VALUES (
            'resource/'||gen_random_uuid()::text,requested_host,requested_operation,
            context->>'input_digest',requested_deployment,requested_bot,
            requested_generation,'POSTGRES_DATABASE',
            'forest_bot_'||substr(name_digest,1,24),
            'forest_role_'||substr(name_digest,1,24),
            'marker/'||replace(gen_random_uuid()::text,'-','')||
                replace(gen_random_uuid()::text,'-',''),'RESERVED')
        RETURNING * INTO item;
    END IF;
    RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',true,
        'value',jsonb_build_object(
            'resource_id',item.resource_id,'host_id',item.host_id,
            'deployment_id',item.deployment_id,'bot_id',item.bot_id,
            'storage_generation',item.storage_generation,'state',item.state,
            'revision',item.revision));
END;
$$;

CREATE FUNCTION host_storage_realize(context jsonb, input jsonb)
RETURNS jsonb LANGUAGE plpgsql VOLATILE SET search_path = pg_catalog AS $$
DECLARE
    requested_operation text := context->>'operation_id';
    requested_resource text := input->>'resource_id';
    item __forest_module_schema__.physical_generations%ROWTYPE;
    replay __forest_module_schema__.realization_operations%ROWTYPE;
    expected_marker_digest text;
    result_value jsonb;
BEGIN
    IF context->>'scope_kind' IS DISTINCT FROM 'host'
       OR context->>'component_id' IS DISTINCT FROM 'host.storage'
       OR context->>'port_kind' IS DISTINCT FROM 'command'
       OR context->>'port_name' IS DISTINCT FROM 'host.storage.realize-record'
       OR requested_operation IS NULL
       OR octet_length(requested_operation) NOT BETWEEN 1 AND 256
       OR context->>'input_digest' !~ '^sha256:[a-f0-9]{64}$'
       OR requested_resource !~ '^resource/[a-f0-9-]{36}$'
       OR input->>'protocol' IS DISTINCT FROM 'forest.host-storage-materialization/1'
       OR input->>'credential_digest' IS NULL
       OR input->>'credential_digest' !~ '^sha256:[a-f0-9]{64}$' THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object('code','HOST_STORAGE_REALIZE_INVALID',
                'message','Host storage realization is invalid','status',422));
    END IF;
    PERFORM pg_advisory_xact_lock(hashtextextended(
        'host.storage.realize/'||(context->>'scope_id')||'/'||requested_operation,0));
    SELECT * INTO replay FROM __forest_module_schema__.realization_operations
    WHERE operation_id=requested_operation;
    IF FOUND THEN
        IF replay.resource_id IS DISTINCT FROM requested_resource
           OR replay.input_digest IS DISTINCT FROM context->>'input_digest' THEN
            RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
                'error',jsonb_build_object('code','HOST_STORAGE_REALIZE_OPERATION_CONFLICT',
                    'message','Realization operation input differs','status',409));
        END IF;
        RETURN replay.result_payload;
    END IF;
    SELECT * INTO item FROM __forest_module_schema__.physical_generations
    WHERE resource_id=requested_resource FOR UPDATE;
    IF NOT FOUND OR item.host_id IS DISTINCT FROM context->>'scope_id'
       OR item.kind IS DISTINCT FROM 'POSTGRES_DATABASE'
       OR item.state NOT IN ('RESERVED','ACTIVE') THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object('code','HOST_STORAGE_RESOURCE_UNAVAILABLE',
                'message','Host PostgreSQL reservation is unavailable','status',409));
    END IF;
    expected_marker_digest := 'sha256:' || encode(sha256(convert_to(
        'forest.host-storage/1|'||item.resource_id||'|'||item.host_id||'|'
            ||item.owner_marker,'UTF8')),'hex');
    IF input->>'host_id' IS DISTINCT FROM item.host_id
       OR input->>'physical_name' IS DISTINCT FROM item.physical_name
       OR input->>'role_name' IS DISTINCT FROM item.role_name
       OR input->>'owner_marker_digest' IS DISTINCT FROM expected_marker_digest THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object('code','HOST_STORAGE_RECEIPT_MISMATCH',
                'message','Physical PostgreSQL receipt differs from reservation','status',409));
    END IF;
    IF item.state='RESERVED' THEN
        UPDATE __forest_module_schema__.physical_generations
        SET state='ACTIVE',revision=revision+1,updated_at=clock_timestamp()
        WHERE resource_id=item.resource_id RETURNING * INTO item;
    END IF;
    result_value := jsonb_build_object('protocol','forest.domain-result/1','ok',true,
        'value',jsonb_build_object('protocol','forest.host-storage-realized/1',
            'resource_id',item.resource_id,'host_id',item.host_id,
            'state',item.state,'revision',item.revision,
            'credential_digest',input->>'credential_digest'));
    INSERT INTO __forest_module_schema__.realization_operations(
        operation_id,resource_id,input_digest,result_payload)
    VALUES(requested_operation,item.resource_id,context->>'input_digest',result_value);
    RETURN result_value;
END;
$$;

CREATE FUNCTION host_storage_enqueue_materialization(context jsonb, input jsonb)
RETURNS jsonb LANGUAGE plpgsql VOLATILE SET search_path = pg_catalog AS $$
DECLARE
    requested_resource text := input->>'resource_id';
    requested_operation text := context->>'operation_id';
    item __forest_module_schema__.physical_generations%ROWTYPE;
    work __forest_module_schema__.materialization_work%ROWTYPE;
BEGIN
    IF context->>'scope_kind' IS DISTINCT FROM 'host'
       OR context->>'component_id' IS DISTINCT FROM 'host.storage'
       OR context->>'port_kind' IS DISTINCT FROM 'command'
       OR context->>'port_name' IS DISTINCT FROM 'host.storage.materialization-enqueue-record'
       OR requested_resource !~ '^resource/[a-f0-9-]{36}$'
       OR requested_operation IS NULL OR octet_length(requested_operation) NOT BETWEEN 1 AND 256
       OR context->>'input_digest' !~ '^sha256:[a-f0-9]{64}$'
       OR input->>'credential_ref' IS NULL
       OR octet_length(input->>'credential_ref') NOT BETWEEN 10 AND 512 THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object('code','HOST_STORAGE_MATERIALIZATION_INVALID',
                'message','Host materialization request is invalid','status',422));
    END IF;
    SELECT * INTO item FROM __forest_module_schema__.physical_generations
    WHERE resource_id=requested_resource FOR UPDATE;
    IF NOT FOUND OR item.host_id IS DISTINCT FROM context->>'scope_id'
       OR item.kind<>'POSTGRES_DATABASE' THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object('code','HOST_STORAGE_RESOURCE_UNAVAILABLE',
                'message','Host reservation is unavailable','status',409));
    END IF;
    SELECT * INTO work FROM __forest_module_schema__.materialization_work
    WHERE resource_id=requested_resource FOR UPDATE;
    IF FOUND THEN
        IF work.request_operation_id IS DISTINCT FROM requested_operation
           OR work.request_digest IS DISTINCT FROM context->>'input_digest' THEN
            RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
                'error',jsonb_build_object('code','HOST_STORAGE_MATERIALIZATION_CONFLICT',
                    'message','Resource has another materialization request','status',409));
        END IF;
    ELSE
        IF item.state<>'RESERVED' THEN
            RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
                'error',jsonb_build_object('code','HOST_STORAGE_RESOURCE_UNAVAILABLE',
                    'message','Reservation is no longer pending','status',409));
        END IF;
        INSERT INTO __forest_module_schema__.materialization_work(
            resource_id,request_operation_id,request_digest,credential_ref)
        VALUES(requested_resource,requested_operation,context->>'input_digest',
            input->>'credential_ref') RETURNING * INTO work;
    END IF;
    RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',true,
        'value',jsonb_build_object('resource_id',work.resource_id,'state',work.state,
            'attempt_count',work.attempt_count,'last_error_code',work.last_error_code));
END;
$$;

CREATE FUNCTION host_storage_claim_materialization(context jsonb, input jsonb)
RETURNS jsonb LANGUAGE plpgsql VOLATILE SET search_path = pg_catalog AS $$
DECLARE
    requested_token text := input->>'claim_token';
    requested_lease integer := (input->>'lease_seconds')::integer;
    work __forest_module_schema__.materialization_work%ROWTYPE;
BEGIN
    IF context->>'scope_kind' IS DISTINCT FROM 'host'
       OR context->>'component_id' IS DISTINCT FROM 'host.storage'
       OR input->>'protocol' IS DISTINCT FROM 'forest.owned-work-claim-request/1'
       OR requested_token IS NULL OR octet_length(requested_token) NOT BETWEEN 1 AND 256
       OR requested_lease IS NULL OR requested_lease NOT BETWEEN 3 AND 86400 THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object('code','HOST_STORAGE_CLAIM_INVALID',
                'message','Materialization claim is invalid','status',422));
    END IF;
    WITH expired AS MATERIALIZED (
        SELECT w.resource_id FROM __forest_module_schema__.materialization_work w
        JOIN __forest_module_schema__.physical_generations p
          ON p.resource_id=w.resource_id
        WHERE p.host_id=context->>'scope_id' AND w.state='CLAIMED'
          AND w.lease_deadline<=clock_timestamp()
        ORDER BY w.lease_deadline,w.resource_id
        FOR UPDATE OF w SKIP LOCKED LIMIT 32
    )
    UPDATE __forest_module_schema__.materialization_work w
    SET state='PENDING',claim_token=NULL,lease_deadline=NULL,
        next_attempt_at=clock_timestamp(),last_error_code='HOST_STORAGE_LEASE_EXPIRED',
        updated_at=clock_timestamp()
    FROM expired e WHERE w.resource_id=e.resource_id;
    SELECT w.* INTO work FROM __forest_module_schema__.materialization_work w
    JOIN __forest_module_schema__.physical_generations p
      ON p.resource_id=w.resource_id
    WHERE p.host_id=context->>'scope_id' AND w.state='PENDING'
      AND w.next_attempt_at<=clock_timestamp()
    ORDER BY w.next_attempt_at,w.resource_id
    FOR UPDATE OF w SKIP LOCKED LIMIT 1;
    IF NOT FOUND THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',true,'value',NULL);
    END IF;
    UPDATE __forest_module_schema__.materialization_work
    SET state='CLAIMED',claim_token=requested_token,
        lease_deadline=clock_timestamp()+(requested_lease*interval '1 second'),
        attempt_count=attempt_count+1,updated_at=clock_timestamp()
    WHERE resource_id=work.resource_id RETURNING * INTO work;
    RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',true,
        'value',jsonb_build_object('protocol','forest.owned-work-claim/1',
            'work_id',work.resource_id,'claim_token',work.claim_token,
            'attempt',work.attempt_count,
            'operation_id','host-materialize:'||encode(sha256(convert_to(
                work.resource_id||'/'||work.attempt_count,'UTF8')),'hex'),
            'causal_root_kind','host.storage.materialization',
            'causal_root_id',work.resource_id,
            'payload',jsonb_build_object('resource_id',work.resource_id,
                'credential_ref',work.credential_ref)));
EXCEPTION WHEN invalid_text_representation OR numeric_value_out_of_range THEN
    RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
        'error',jsonb_build_object('code','HOST_STORAGE_CLAIM_INVALID',
            'message','Materialization claim is invalid','status',422));
END;
$$;

CREATE FUNCTION host_storage_settle_materialization(context jsonb, input jsonb)
RETURNS jsonb LANGUAGE plpgsql VOLATILE SET search_path = pg_catalog AS $$
DECLARE
    work __forest_module_schema__.materialization_work%ROWTYPE;
    replay __forest_module_schema__.materialization_settlements%ROWTYPE;
    item __forest_module_schema__.physical_generations%ROWTYPE;
    outcome jsonb := input->'outcome';
    result_value jsonb;
BEGIN
    IF context->>'scope_kind' IS DISTINCT FROM 'host'
       OR context->>'component_id' IS DISTINCT FROM 'host.storage'
       OR input->>'protocol' IS DISTINCT FROM 'forest.owned-work-settle-request/1' THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object('code','HOST_STORAGE_SETTLEMENT_INVALID',
                'message','Materialization settlement is invalid','status',422));
    END IF;
    SELECT * INTO replay FROM __forest_module_schema__.materialization_settlements
    WHERE claim_token=input->>'claim_token';
    IF FOUND THEN
        IF replay.input_payload=input THEN RETURN replay.result_payload; END IF;
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object('code','HOST_STORAGE_SETTLEMENT_CONFLICT',
                'message','Claim was settled differently','status',409));
    END IF;
    SELECT * INTO work FROM __forest_module_schema__.materialization_work
    WHERE resource_id=input->>'work_id' FOR UPDATE;
    IF NOT FOUND OR work.state<>'CLAIMED'
       OR work.claim_token IS DISTINCT FROM input->>'claim_token'
       OR work.attempt_count IS DISTINCT FROM (input->>'attempt')::integer THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object('code','HOST_STORAGE_CLAIM_LOST',
                'message','Materialization claim is no longer current','status',409));
    END IF;
    SELECT * INTO item FROM __forest_module_schema__.physical_generations
    WHERE resource_id=work.resource_id AND host_id=context->>'scope_id' FOR UPDATE;
    IF NOT FOUND THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object('code','HOST_STORAGE_RESOURCE_UNAVAILABLE',
                'message','Host reservation is unavailable','status',409));
    END IF;
    IF outcome->>'status'='SUCCEEDED' THEN
        IF item.state<>'ACTIVE'
           OR outcome#>>'{result,protocol}' IS DISTINCT FROM 'forest.host-storage-realized/1'
           OR outcome#>>'{result,resource_id}' IS DISTINCT FROM work.resource_id
           OR outcome#>>'{result,host_id}' IS DISTINCT FROM item.host_id
           OR (outcome#>>'{result,revision}')::bigint IS DISTINCT FROM item.revision
           OR outcome#>>'{result,credential_digest}' IS NULL
           OR outcome#>>'{result,credential_digest}' !~ '^sha256:[a-f0-9]{64}$' THEN
            RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
                'error',jsonb_build_object('code','HOST_STORAGE_SETTLEMENT_INVALID',
                    'message','Realization receipt differs from Host ledger','status',409));
        END IF;
        UPDATE __forest_module_schema__.materialization_work
        SET state='SUCCEEDED',claim_token=NULL,lease_deadline=NULL,
            last_error_code=NULL,result_payload=outcome->'result',updated_at=clock_timestamp()
        WHERE resource_id=work.resource_id;
    ELSE
        UPDATE __forest_module_schema__.materialization_work
        SET state=CASE WHEN outcome#>>'{error,retryable}'='true'
                        OR outcome#>>'{error,unknown}'='true'
                       THEN 'PENDING' ELSE 'FAILED' END,
            claim_token=NULL,lease_deadline=NULL,
            next_attempt_at=clock_timestamp()+
                (LEAST(power(2,LEAST(work.attempt_count,8)),300)*interval '1 second'),
            last_error_code=COALESCE(outcome#>>'{error,code}',
                'HOST_STORAGE_MATERIALIZATION_UNKNOWN'),updated_at=clock_timestamp()
        WHERE resource_id=work.resource_id;
    END IF;
    result_value := jsonb_build_object('protocol','forest.domain-result/1','ok',true,
        'value',jsonb_build_object('protocol','forest.owned-work-settled/1',
            'work_id',work.resource_id));
    INSERT INTO __forest_module_schema__.materialization_settlements(
        claim_token,resource_id,input_payload,result_payload)
    VALUES(input->>'claim_token',work.resource_id,input,result_value);
    RETURN result_value;
EXCEPTION WHEN invalid_text_representation OR numeric_value_out_of_range THEN
    RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
        'error',jsonb_build_object('code','HOST_STORAGE_SETTLEMENT_INVALID',
            'message','Materialization settlement is invalid','status',422));
END;
$$;

CREATE FUNCTION host_storage_pin_reference(context jsonb, input jsonb)
RETURNS jsonb LANGUAGE plpgsql VOLATILE SET search_path = pg_catalog AS $$
DECLARE
    requested_operation text := context->>'operation_id';
    requested_resource text := input->>'resource_id';
    item __forest_module_schema__.physical_generations%ROWTYPE;
    work __forest_module_schema__.materialization_work%ROWTYPE;
    replay __forest_module_schema__.resource_reference_pins%ROWTYPE;
    result_value jsonb;
BEGIN
    IF context->>'scope_kind' IS DISTINCT FROM 'host'
       OR context->>'component_id' IS DISTINCT FROM 'host.storage'
       OR context->>'port_kind' IS DISTINCT FROM 'command'
       OR context->>'port_name' IS DISTINCT FROM 'host.storage.reference-pin-record'
       OR requested_operation IS NULL OR octet_length(requested_operation) NOT BETWEEN 1 AND 256
       OR context->>'input_digest' !~ '^sha256:[a-f0-9]{64}$'
       OR requested_resource !~ '^resource/[a-f0-9-]{36}$'
       OR input->>'deployment_id' IS NULL OR input->>'bot_id' IS NULL
       OR input->>'storage_generation' IS NULL
       OR input->>'profile_ref' IS NULL
       OR octet_length(input->>'profile_ref') NOT BETWEEN 1 AND 512 THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object('code','HOST_STORAGE_REFERENCE_INVALID',
                'message','Host reference pin is invalid','status',422));
    END IF;
    PERFORM pg_advisory_xact_lock(hashtextextended(
        'host.storage.reference.operation/'||requested_operation,0));
    SELECT * INTO replay FROM __forest_module_schema__.resource_reference_pins
    WHERE operation_id=requested_operation FOR UPDATE;
    IF FOUND THEN
        IF replay.resource_id IS DISTINCT FROM requested_resource
           OR replay.input_digest IS DISTINCT FROM context->>'input_digest' THEN
            RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
                'error',jsonb_build_object('code','HOST_STORAGE_REFERENCE_CONFLICT',
                    'message','Reference operation differs','status',409));
        END IF;
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',true,
            'value',replay.result_payload);
    END IF;
    SELECT * INTO item FROM __forest_module_schema__.physical_generations
    WHERE resource_id=requested_resource FOR UPDATE;
    IF NOT FOUND OR item.host_id IS DISTINCT FROM context->>'scope_id'
       OR item.deployment_id IS DISTINCT FROM input->>'deployment_id'
       OR item.bot_id IS DISTINCT FROM input->>'bot_id'
       OR item.storage_generation IS DISTINCT FROM (input->>'storage_generation')::bigint
       OR item.state<>'ACTIVE'
       OR NOT EXISTS (
           SELECT 1 FROM __forest_module_schema__.materialization_work w
           WHERE w.resource_id=item.resource_id AND w.state='SUCCEEDED'
             AND (w.result_payload->>'revision')::bigint=item.revision
       ) THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object('code','HOST_STORAGE_RESOURCE_UNAVAILABLE',
                'message','Active Host resource is unavailable for reference','status',409));
    END IF;
    SELECT * INTO work FROM __forest_module_schema__.materialization_work
    WHERE resource_id=item.resource_id AND state='SUCCEEDED';
    IF NOT FOUND OR work.result_payload->>'credential_digest' IS NULL
       OR work.result_payload->>'credential_digest' !~ '^sha256:[a-f0-9]{64}$' THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object('code','HOST_STORAGE_RESOURCE_UNAVAILABLE',
                'message','Active Host resource has no credential witness','status',409));
    END IF;
    PERFORM pg_advisory_xact_lock(hashtextextended(
        'host.storage.reference.profile/'||item.deployment_id||'/'||(input->>'profile_ref'),0));
    SELECT * INTO replay FROM __forest_module_schema__.resource_reference_pins
    WHERE resource_id=requested_resource
       OR (deployment_id=item.deployment_id AND profile_ref=input->>'profile_ref')
    FOR UPDATE;
    IF FOUND THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object('code','HOST_STORAGE_REFERENCE_CONFLICT',
                'message','Resource or profile already has another reference','status',409));
    END IF;
    result_value := jsonb_build_object(
        'resource_id',item.resource_id,'host_id',item.host_id,
        'deployment_id',item.deployment_id,'bot_id',item.bot_id,
        'storage_generation',item.storage_generation,
        'profile_ref',input->>'profile_ref','state','PINNED',
        'resource_revision',item.revision,
        'credential_digest',work.result_payload->>'credential_digest');
    INSERT INTO __forest_module_schema__.resource_reference_pins(
        resource_id,deployment_id,operation_id,input_digest,profile_ref,
        resource_revision,result_payload)
    VALUES(item.resource_id,item.deployment_id,requested_operation,
        context->>'input_digest',input->>'profile_ref',item.revision,result_value);
    RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',true,
        'value',result_value);
EXCEPTION WHEN invalid_text_representation OR numeric_value_out_of_range THEN
    RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
        'error',jsonb_build_object('code','HOST_STORAGE_REFERENCE_INVALID',
            'message','Host reference pin is invalid','status',422));
END;
$$;

CREATE FUNCTION host_storage_materialization_status(context jsonb, input jsonb)
RETURNS jsonb LANGUAGE plpgsql STABLE SET search_path = pg_catalog AS $$
DECLARE
    work __forest_module_schema__.materialization_work%ROWTYPE;
BEGIN
    IF context->>'scope_kind' IS DISTINCT FROM 'host'
       OR context->>'component_id' IS DISTINCT FROM 'host.storage'
       OR context->>'port_kind' IS DISTINCT FROM 'query'
       OR context->>'port_name' IS DISTINCT FROM 'host.storage.materialization-status-read'
       OR input->>'resource_id' !~ '^resource/[a-f0-9-]{36}$'
       OR input->>'deployment_id' IS NULL
       OR input->>'bot_id' IS NULL
       OR input->>'storage_generation' IS NULL THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object('code','HOST_STORAGE_QUERY_INVALID',
                'message','Materialization status query is invalid','status',422));
    END IF;
    SELECT w.* INTO work FROM __forest_module_schema__.materialization_work w
    JOIN __forest_module_schema__.physical_generations p
      ON p.resource_id=w.resource_id
    WHERE w.resource_id=input->>'resource_id' AND p.host_id=context->>'scope_id'
      AND p.deployment_id=input->>'deployment_id' AND p.bot_id=input->>'bot_id'
      AND p.storage_generation=(input->>'storage_generation')::bigint;
    IF NOT FOUND THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object('code','HOST_STORAGE_MATERIALIZATION_NOT_FOUND',
                'message','Materialization request was not found','status',404));
    END IF;
    RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',true,
        'value',jsonb_build_object('resource_id',work.resource_id,'state',work.state,
            'attempt_count',work.attempt_count,'last_error_code',work.last_error_code));
END;
$$;

CREATE FUNCTION host_storage_resource(context jsonb, input jsonb)
RETURNS jsonb LANGUAGE plpgsql STABLE SET search_path = pg_catalog AS $$
DECLARE
    item __forest_module_schema__.physical_generations%ROWTYPE;
BEGIN
    IF context->>'scope_kind' IS DISTINCT FROM 'host'
       OR context->>'component_id' IS DISTINCT FROM 'host.storage'
       OR context->>'port_kind' IS DISTINCT FROM 'query'
       OR context->>'port_name' IS DISTINCT FROM 'host.storage.resource-read'
       OR input->>'resource_id' !~ '^resource/[a-f0-9-]{36}$'
       OR input->>'deployment_id' IS NULL
       OR input->>'bot_id' IS NULL
       OR input->>'storage_generation' IS NULL THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object('code','HOST_STORAGE_QUERY_INVALID',
                'message','Host resource query is invalid','status',422));
    END IF;
    SELECT * INTO item FROM __forest_module_schema__.physical_generations
    WHERE resource_id=input->>'resource_id' AND host_id=context->>'scope_id'
      AND deployment_id=input->>'deployment_id' AND bot_id=input->>'bot_id'
      AND storage_generation=(input->>'storage_generation')::bigint;
    IF NOT FOUND THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object('code','HOST_STORAGE_RESOURCE_NOT_FOUND',
                'message','Host resource was not found','status',404));
    END IF;
    RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',true,
        'value',jsonb_build_object('resource_id',item.resource_id,'host_id',item.host_id,
            'deployment_id',item.deployment_id,'bot_id',item.bot_id,
            'storage_generation',item.storage_generation,'kind',item.kind,
            'physical_name',item.physical_name,'role_name',item.role_name,
            'state',item.state,'revision',item.revision));
EXCEPTION WHEN invalid_text_representation OR numeric_value_out_of_range THEN
    RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
        'error',jsonb_build_object('code','HOST_STORAGE_QUERY_INVALID',
            'message','Host resource query is invalid','status',422));
END;
$$;

CREATE FUNCTION host_storage_generations(context jsonb, input jsonb)
RETURNS jsonb LANGUAGE plpgsql STABLE SET search_path = pg_catalog AS $$
DECLARE
    page_limit integer := COALESCE((input->>'limit')::integer,50);
    rows jsonb;
    next_cursor text;
BEGIN
    IF context->>'scope_kind' IS DISTINCT FROM 'host'
       OR context->>'component_id' IS DISTINCT FROM 'host.storage'
       OR context->>'port_kind' IS DISTINCT FROM 'query'
       OR context->>'port_name' IS DISTINCT FROM 'host.storage.generations.read'
       OR page_limit NOT BETWEEN 1 AND 100 THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object('code','HOST_STORAGE_QUERY_INVALID',
                'message','Host storage query is invalid','status',422));
    END IF;
    SELECT COALESCE(jsonb_agg(jsonb_build_object(
        'resource_id',page.resource_id,'host_id',page.host_id,
        'deployment_id',page.deployment_id,'bot_id',page.bot_id,
        'storage_generation',page.storage_generation,'kind',page.kind,
        'physical_name',page.physical_name,'role_name',page.role_name,
        'state',page.state,'revision',page.revision)
        ORDER BY page.resource_id),'[]'::jsonb)
    INTO rows
    FROM (
        SELECT resource_id,host_id,deployment_id,bot_id,storage_generation,
            kind,physical_name,role_name,state,revision
        FROM __forest_module_schema__.physical_generations
        WHERE host_id=context->>'scope_id'
          AND (input->>'after' IS NULL OR resource_id>input->>'after')
          AND (input->>'deployment_id' IS NULL OR deployment_id=input->>'deployment_id')
          AND (input->>'bot_id' IS NULL OR bot_id=input->>'bot_id')
          AND (input->>'state' IS NULL OR state=input->>'state')
        ORDER BY resource_id LIMIT page_limit+1
    ) AS page;
    IF jsonb_array_length(rows)>page_limit THEN
        rows := rows-page_limit;
        next_cursor := rows->(page_limit-1)->>'resource_id';
    END IF;
    RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',true,
        'value',jsonb_build_object('items',rows,'next_after',next_cursor));
END;
$$;
