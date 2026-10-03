CREATE TABLE package_import_requests (
    operation_id text PRIMARY KEY,
    actor_ref text NOT NULL,
    generation_id text NOT NULL,
    content_length integer NOT NULL CHECK (content_length BETWEEN 1 AND 33554432),
    archive_sha256 text NOT NULL CHECK (archive_sha256 ~ '^sha256:[a-f0-9]{64}$'),
    status text NOT NULL CHECK (status IN ('RESERVED','SUCCEEDED','REJECTED','INTERRUPTED')),
    package_digest text CHECK (package_digest ~ '^sha256:[a-f0-9]{64}$'),
    error_code text CHECK (error_code ~ '^[A-Z][A-Z0-9_]{0,127}$'),
    recovery_token text,
    recovery_attempt integer NOT NULL DEFAULT 0 CHECK (recovery_attempt >= 0),
    recovery_lease_deadline timestamptz,
    recovery_next_attempt_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    recovery_error_code text CHECK (recovery_error_code ~ '^[A-Z][A-Z0-9_]{0,127}$'),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    CHECK ((status = 'SUCCEEDED') = (package_digest IS NOT NULL)),
    CHECK ((status = 'REJECTED') = (error_code IS NOT NULL))
);

CREATE INDEX package_import_requests_page
    ON package_import_requests (created_at DESC, operation_id DESC);

CREATE INDEX package_import_requests_recovery
    ON package_import_requests (recovery_next_attempt_at, operation_id)
    WHERE status='RESERVED';

CREATE TABLE package_import_recovery_settlements (
    claim_token text PRIMARY KEY,
    input_payload jsonb NOT NULL,
    result_payload jsonb NOT NULL
);

CREATE FUNCTION package_import_reserve(context jsonb, input jsonb)
RETURNS jsonb LANGUAGE plpgsql VOLATILE SET search_path = pg_catalog AS $$
DECLARE
    item record;
BEGIN
    IF context->>'scope_kind' IS DISTINCT FROM 'deployment'
       OR context->>'component_id' IS DISTINCT FROM 'deployment.package-registry'
       OR context->>'port_kind' IS DISTINCT FROM 'management_action'
       OR context->>'port_name' IS DISTINCT FROM 'deployment.package-registry.import.reserve'
       OR context->>'operation_id' IS DISTINCT FROM input->>'operation_id'
       OR context->>'generation_id' IS DISTINCT FROM input->>'generation_id' THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object('code','PACKAGE_IMPORT_CONTEXT_INVALID',
                'message','Import reservation has no matching Generation authority','status',403));
    END IF;
    INSERT INTO __forest_module_schema__.package_import_requests(
        operation_id,actor_ref,generation_id,content_length,archive_sha256,status)
    VALUES (input->>'operation_id',input->>'actor_ref',input->>'generation_id',
            (input->>'content_length')::integer,input->>'archive_sha256','RESERVED')
    ON CONFLICT (operation_id) DO NOTHING;
    SELECT * INTO item FROM __forest_module_schema__.package_import_requests
        WHERE operation_id=input->>'operation_id' FOR UPDATE;
    IF item.actor_ref IS DISTINCT FROM input->>'actor_ref'
       OR item.generation_id IS DISTINCT FROM input->>'generation_id'
       OR item.content_length IS DISTINCT FROM (input->>'content_length')::integer
       OR item.archive_sha256 IS DISTINCT FROM input->>'archive_sha256' THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object('code','PACKAGE_IMPORT_OPERATION_CONFLICT',
                'message','Import operation identity differs from its reservation','status',409));
    END IF;
    RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',true,
        'value',jsonb_build_object('operation_id',item.operation_id,'status',item.status));
END;
$$;

CREATE FUNCTION package_import_settle(context jsonb, input jsonb)
RETURNS jsonb LANGUAGE plpgsql VOLATILE SET search_path = pg_catalog AS $$
DECLARE
    item record;
BEGIN
    IF context->>'scope_kind' IS DISTINCT FROM 'deployment'
       OR context->>'component_id' IS DISTINCT FROM 'deployment.package-registry'
       OR context->>'port_kind' IS DISTINCT FROM 'management_action'
       OR context->>'port_name' IS DISTINCT FROM 'deployment.package-registry.import.settle'
       OR context->>'operation_id' IS DISTINCT FROM input->>'operation_id' THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object('code','PACKAGE_IMPORT_CONTEXT_INVALID',
                'message','Import settlement has no matching authority','status',403));
    END IF;
    SELECT * INTO item FROM __forest_module_schema__.package_import_requests
        WHERE operation_id=input->>'operation_id' FOR UPDATE;
    IF NOT FOUND OR item.actor_ref IS DISTINCT FROM input->>'actor_ref'
       OR item.archive_sha256 IS DISTINCT FROM input->>'archive_sha256' THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object('code','PACKAGE_IMPORT_OPERATION_CONFLICT',
                'message','Import settlement differs from its reservation','status',409));
    END IF;
    IF item.status = 'RESERVED' THEN
        UPDATE __forest_module_schema__.package_import_requests SET
            status=input->>'status',package_digest=input->>'package_digest',
            error_code=input->>'error_code',updated_at=clock_timestamp()
        WHERE operation_id=item.operation_id;
    ELSIF item.status IS DISTINCT FROM input->>'status'
       OR item.package_digest IS DISTINCT FROM input->>'package_digest'
       OR item.error_code IS DISTINCT FROM input->>'error_code' THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object('code','PACKAGE_IMPORT_SETTLEMENT_CONFLICT',
                'message','Import already has a different terminal result','status',409));
    END IF;
    RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',true,
        'value',jsonb_build_object('operation_id',item.operation_id,
            'status',input->>'status','package_digest',input->>'package_digest',
            'error_code',input->>'error_code'));
END;
$$;

CREATE FUNCTION package_import_page(context jsonb, input jsonb)
RETURNS jsonb LANGUAGE plpgsql STABLE SET search_path = pg_catalog AS $$
DECLARE
    cursor_time timestamptz;
    cursor_id text;
    page_limit integer := COALESCE((input->>'limit')::integer, 32);
    records jsonb;
    next_before text;
BEGIN
    IF context->>'scope_kind' IS DISTINCT FROM 'deployment'
       OR context->>'component_id' IS DISTINCT FROM 'deployment.package-registry'
       OR context->>'port_name' IS DISTINCT FROM 'deployment.package-registry.import.page'
       OR page_limit NOT BETWEEN 1 AND 100 THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object('code','PACKAGE_IMPORT_QUERY_INVALID',
                'message','Import query is invalid','status',422));
    END IF;
    IF input ? 'before' THEN
        SELECT created_at,operation_id INTO cursor_time,cursor_id
        FROM __forest_module_schema__.package_import_requests
        WHERE operation_id=input->>'before';
        IF NOT FOUND THEN
            RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
                'error',jsonb_build_object('code','PACKAGE_IMPORT_CURSOR_NOT_FOUND',
                    'message','Import cursor is unavailable','status',404));
        END IF;
    END IF;
    SELECT COALESCE(jsonb_agg(to_jsonb(page) ORDER BY page.created_at DESC,
        page.operation_id DESC),'[]'::jsonb) INTO records
    FROM (SELECT item.operation_id,item.actor_ref,item.generation_id,
                 item.content_length,item.archive_sha256,item.status,
                 item.package_digest,item.error_code,item.recovery_error_code,
                 item.created_at,item.updated_at
          FROM __forest_module_schema__.package_import_requests AS item
        WHERE (input->>'operation_id' IS NULL OR item.operation_id=input->>'operation_id')
          AND (input->>'actor_ref' IS NULL OR item.actor_ref=input->>'actor_ref')
          AND (input->>'status' IS NULL OR item.status=input->>'status')
          AND (input->>'package_digest' IS NULL OR item.package_digest=input->>'package_digest')
          AND (cursor_id IS NULL OR (item.created_at,item.operation_id)<(cursor_time,cursor_id))
        ORDER BY item.created_at DESC,item.operation_id DESC LIMIT page_limit+1) AS page;
    IF jsonb_array_length(records)>page_limit THEN
        records := records - page_limit;
        next_before := records->(page_limit-1)->>'operation_id';
    END IF;
    RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',true,
        'value',jsonb_build_object('records',records,'next_before',next_before));
END;
$$;

CREATE FUNCTION package_import_recovery_claim(context jsonb, input jsonb)
RETURNS jsonb LANGUAGE plpgsql VOLATILE SET search_path = pg_catalog AS $$
DECLARE
    requested_token text := input->>'claim_token';
    requested_lease integer := (input->>'lease_seconds')::integer;
    item __forest_module_schema__.package_import_requests%ROWTYPE;
BEGIN
    IF context->>'scope_kind' IS DISTINCT FROM 'deployment'
       OR context->>'component_id' IS DISTINCT FROM 'deployment.package-registry'
       OR context->>'port_kind' IS DISTINCT FROM 'owned_claim'
       OR context->>'port_name' IS DISTINCT FROM 'deployment.package-registry.import.recover.claim'
       OR input->>'protocol' IS DISTINCT FROM 'forest.owned-work-claim-request/1'
       OR requested_token IS NULL OR octet_length(requested_token) NOT BETWEEN 1 AND 256
       OR requested_lease IS NULL OR requested_lease NOT BETWEEN 3 AND 86400 THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object('code','PACKAGE_IMPORT_RECOVERY_CLAIM_INVALID',
                'message','Import recovery claim is invalid','status',422));
    END IF;
    SELECT * INTO item FROM __forest_module_schema__.package_import_requests
    WHERE status='RESERVED' AND recovery_token=requested_token FOR UPDATE;
    IF NOT FOUND THEN
        SELECT * INTO item FROM __forest_module_schema__.package_import_requests
        WHERE status='RESERVED'
          AND created_at <= clock_timestamp()-interval '30 seconds'
          AND recovery_next_attempt_at <= clock_timestamp()
          AND (recovery_token IS NULL OR recovery_lease_deadline <= clock_timestamp())
          AND recovery_attempt < 2147483647
        ORDER BY recovery_next_attempt_at,operation_id
        FOR UPDATE SKIP LOCKED LIMIT 1;
        IF NOT FOUND THEN
            RETURN jsonb_build_object('protocol','forest.domain-result/1',
                'ok',true,'value',NULL);
        END IF;
        UPDATE __forest_module_schema__.package_import_requests
        SET recovery_token=requested_token,
            recovery_attempt=recovery_attempt+1,
            recovery_lease_deadline=clock_timestamp()
                +(requested_lease * interval '1 second')
        WHERE operation_id=item.operation_id RETURNING * INTO item;
    END IF;
    RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',true,
        'value',jsonb_build_object(
            'protocol','forest.owned-work-claim/1',
            'work_id',item.operation_id,
            'claim_token',item.recovery_token,
            'attempt',item.recovery_attempt,
            'operation_id','recover:'||md5(item.operation_id||':'||item.recovery_attempt::text),
            'causal_root_kind','deployment.package-import',
            'causal_root_id',item.operation_id,
            'payload',jsonb_build_object(
                'operation_id',item.operation_id,
                'actor_ref',item.actor_ref,
                'archive_sha256',item.archive_sha256)));
END;
$$;

CREATE FUNCTION package_import_recovery_settle(context jsonb, input jsonb)
RETURNS jsonb LANGUAGE plpgsql VOLATILE SET search_path = pg_catalog AS $$
DECLARE
    requested_work_id text := input->>'work_id';
    requested_token text := input->>'claim_token';
    requested_attempt integer := (input->>'attempt')::integer;
    outcome jsonb := input->'outcome';
    physical jsonb := outcome->'result';
    prior_input jsonb;
    prior_result jsonb;
    result_value jsonb;
    item __forest_module_schema__.package_import_requests%ROWTYPE;
    next_status text;
    next_digest text;
BEGIN
    IF context->>'scope_kind' IS DISTINCT FROM 'deployment'
       OR context->>'component_id' IS DISTINCT FROM 'deployment.package-registry'
       OR context->>'port_kind' IS DISTINCT FROM 'owned_settle'
       OR context->>'port_name' IS DISTINCT FROM 'deployment.package-registry.import.recover.settle'
       OR input->>'protocol' IS DISTINCT FROM 'forest.owned-work-settle-request/1'
       OR requested_work_id IS NULL OR requested_token IS NULL
       OR requested_attempt IS NULL OR requested_attempt < 1
       OR outcome->>'status' NOT IN ('SUCCEEDED','FAILED')
       OR outcome->>'status' IS NULL THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object('code','PACKAGE_IMPORT_RECOVERY_SETTLE_INVALID',
                'message','Import recovery settlement is invalid','status',422));
    END IF;
    SELECT * INTO item FROM __forest_module_schema__.package_import_requests
        WHERE operation_id=requested_work_id FOR UPDATE;
    IF NOT FOUND THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object('code','PACKAGE_IMPORT_RECOVERY_WORK_NOT_FOUND',
                'message','Import recovery item is unavailable','status',404));
    END IF;
    SELECT input_payload,result_payload INTO prior_input,prior_result
    FROM __forest_module_schema__.package_import_recovery_settlements
    WHERE claim_token=requested_token;
    IF FOUND THEN
        IF prior_input=input THEN RETURN prior_result; END IF;
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object('code','PACKAGE_IMPORT_RECOVERY_SETTLE_CONFLICT',
                'message','Recovery token has another settlement','status',409));
    END IF;
    IF item.recovery_token IS DISTINCT FROM requested_token
       OR item.recovery_attempt IS DISTINCT FROM requested_attempt THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object('code','PACKAGE_IMPORT_RECOVERY_CLAIM_STALE',
                'message','Recovery claim no longer owns this import','status',409));
    END IF;
    IF outcome->>'status' = 'SUCCEEDED' THEN
        IF jsonb_typeof(physical->'found') IS DISTINCT FROM 'boolean'
           OR jsonb_typeof(physical->'closed') IS DISTINCT FROM 'boolean'
           OR (physical->>'found')::boolean = (physical->>'closed')::boolean
           OR ((physical->>'found')::boolean
                AND COALESCE(physical->>'package_digest','') !~ '^sha256:[a-f0-9]{64}$')
           OR (NOT (physical->>'found')::boolean
                AND physical->>'package_digest' IS NOT NULL) THEN
            RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
                'error',jsonb_build_object('code','PACKAGE_IMPORT_RECOVERY_EVIDENCE_INVALID',
                    'message','Physical import evidence is invalid','status',502));
        END IF;
        next_status := CASE WHEN (physical->>'found')::boolean
            THEN 'SUCCEEDED' ELSE 'INTERRUPTED' END;
        next_digest := physical->>'package_digest';
        IF item.status='RESERVED' THEN
            UPDATE __forest_module_schema__.package_import_requests SET
                status=next_status,package_digest=next_digest,error_code=NULL,
                recovery_lease_deadline=NULL,recovery_error_code=NULL,
                updated_at=clock_timestamp()
            WHERE operation_id=item.operation_id;
        ELSIF NOT (
            (next_status='SUCCEEDED' AND item.status='SUCCEEDED'
                AND item.package_digest=next_digest)
            OR (next_status='INTERRUPTED' AND item.status IN ('REJECTED','INTERRUPTED'))
        ) THEN
            RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
                'error',jsonb_build_object('code','PACKAGE_IMPORT_RECOVERY_FACT_CONFLICT',
                    'message','Import terminal state conflicts with physical evidence','status',409));
        END IF;
    ELSE
        UPDATE __forest_module_schema__.package_import_requests SET
            recovery_token=NULL,recovery_lease_deadline=NULL,
            recovery_next_attempt_at=clock_timestamp()+interval '30 seconds',
            recovery_error_code=COALESCE(outcome->'error'->>'code',
                'PACKAGE_IMPORT_RECOVERY_FAILED')
        WHERE operation_id=item.operation_id AND status='RESERVED';
    END IF;
    result_value := jsonb_build_object('protocol','forest.domain-result/1',
        'ok',true,'value',jsonb_build_object(
            'protocol','forest.owned-work-settled/1','work_id',requested_work_id));
    INSERT INTO __forest_module_schema__.package_import_recovery_settlements(
        claim_token,input_payload,result_payload)
    VALUES (requested_token,input,result_value);
    RETURN result_value;
END;
$$;
