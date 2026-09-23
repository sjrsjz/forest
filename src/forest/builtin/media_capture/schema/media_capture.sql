CREATE TABLE media_captures (
    capture_id text PRIMARY KEY CHECK (octet_length(capture_id) BETWEEN 1 AND 256),
    operation_id text NOT NULL UNIQUE CHECK (octet_length(operation_id) BETWEEN 1 AND 256),
    input_payload jsonb NOT NULL,
    identity jsonb NOT NULL,
    source_unit_id text NOT NULL UNIQUE,
    source_revision bigint NOT NULL CHECK (source_revision > 0),
    unit_kind text NOT NULL,
    author_subject_ref text NOT NULL,
    author jsonb NOT NULL,
    binding_ref text NOT NULL,
    conversation_ref text NOT NULL,
    event_ref text NOT NULL,
    ingress_seq bigint NOT NULL CHECK (ingress_seq > 0),
    observed_at timestamptz NOT NULL,
    occurred_at timestamptz NOT NULL,
    state text NOT NULL DEFAULT 'PENDING' CHECK (state IN ('PENDING','CLAIMED','READY','PARTIAL','FAILED','UNKNOWN')),
    attempt_count integer NOT NULL DEFAULT 0 CHECK (attempt_count >= 0),
    claim_token text UNIQUE,
    lease_deadline timestamptz,
    next_attempt_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    settlement jsonb,
    error_code text,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    settled_at timestamptz,
    CHECK ((state='CLAIMED') = (claim_token IS NOT NULL AND lease_deadline IS NOT NULL)),
    CHECK ((state IN ('READY','PARTIAL','FAILED','UNKNOWN')) = (settled_at IS NOT NULL))
);

CREATE TABLE media_capture_parts (
    capture_id text NOT NULL REFERENCES media_captures(capture_id),
    part_id text NOT NULL,
    part_index integer NOT NULL CHECK (part_index >= 0),
    part_kind text NOT NULL CHECK (part_kind IN ('TEXT','IMAGE')),
    state text NOT NULL CHECK (state IN ('PENDING','READY','FAILED')),
    media_ref text UNIQUE,
    locator jsonb,
    payload jsonb,
    media_type text,
    width integer,
    height integer,
    byte_length integer,
    error_code text,
    PRIMARY KEY (capture_id,part_id),
    UNIQUE (capture_id,part_index),
    CHECK (
        (part_kind='TEXT' AND state='READY' AND media_ref IS NULL AND locator IS NULL AND payload IS NOT NULL)
        OR (part_kind='IMAGE' AND media_ref IS NOT NULL AND locator IS NOT NULL)
    )
);

CREATE TABLE media_assets (
    media_ref text NOT NULL,
    media_revision bigint NOT NULL DEFAULT 1 CHECK (media_revision > 0),
    capture_id text NOT NULL,
    part_id text NOT NULL,
    payload jsonb NOT NULL,
    media_type text NOT NULL CHECK (media_type IN ('image/png','image/jpeg','image/webp','image/gif')),
    width integer NOT NULL CHECK (width > 0),
    height integer NOT NULL CHECK (height > 0),
    byte_length integer NOT NULL CHECK (byte_length BETWEEN 1 AND 8388608),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (media_ref,media_revision),
    UNIQUE (capture_id,part_id),
    FOREIGN KEY (capture_id,part_id) REFERENCES media_capture_parts(capture_id,part_id)
);

CREATE TABLE media_command_operations (
    operation_id text PRIMARY KEY,
    port_name text NOT NULL,
    input_payload jsonb NOT NULL,
    result_payload jsonb NOT NULL
);

CREATE TABLE media_work_settlements (
    claim_token text PRIMARY KEY,
    input_payload jsonb NOT NULL,
    result_payload jsonb NOT NULL
);

CREATE INDEX media_capture_work_idx ON media_captures(next_attempt_at,capture_id)
WHERE state='PENDING';

CREATE FUNCTION media_register_ingress(context jsonb, input jsonb)
RETURNS jsonb LANGUAGE plpgsql VOLATILE SET search_path = pg_catalog AS $$
DECLARE
    requested_operation text := context->>'operation_id';
    requested_capture text := input->>'capture_id';
    existing __forest_module_schema__.media_captures%ROWTYPE;
    part jsonb;
    result_value jsonb;
BEGIN
    IF requested_operation IS NULL OR requested_capture IS NULL THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,'error',jsonb_build_object('code','MEDIA_CAPTURE_INPUT_INVALID','message','Media ingress identity is invalid','status',422));
    END IF;
    SELECT * INTO existing FROM __forest_module_schema__.media_captures
    WHERE capture_id=requested_capture OR operation_id=requested_operation FOR UPDATE;
    IF FOUND THEN
        IF existing.capture_id=requested_capture AND existing.operation_id=requested_operation
           AND (existing.input_payload-'observed_at')=(input-'observed_at') THEN
            RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',true,'value',jsonb_build_object('capture_id',requested_capture,'state','PENDING'));
        END IF;
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,'error',jsonb_build_object('code','MEDIA_CAPTURE_CONFLICT','message','Media ingress identity was reused with another input','status',409));
    END IF;
    INSERT INTO __forest_module_schema__.media_captures(
        capture_id,operation_id,input_payload,identity,source_unit_id,source_revision,unit_kind,
        author_subject_ref,author,binding_ref,conversation_ref,event_ref,ingress_seq,observed_at,occurred_at
    ) VALUES (
        requested_capture,requested_operation,input,input->'identity',input->>'source_unit_id',
        (input->>'source_revision')::bigint,input->>'unit_kind',input->>'author_subject_ref',input->'author',
        input->>'binding_ref',input->>'conversation_ref',input->>'event_ref',(input->>'ingress_seq')::bigint,
        (input->>'observed_at')::timestamptz,(input->>'occurred_at')::timestamptz
    );
    FOR part IN SELECT value FROM jsonb_array_elements(input->'text_parts') LOOP
        INSERT INTO __forest_module_schema__.media_capture_parts(
            capture_id,part_id,part_index,part_kind,state,payload,media_type,width,height,byte_length
        ) VALUES (
            requested_capture,part->>'part_id',(part->>'part_index')::integer,'TEXT','READY',part->'payload',
            part->>'media_type',NULL,NULL,(part->>'byte_length')::integer
        );
    END LOOP;
    FOR part IN SELECT value FROM jsonb_array_elements(input->'image_parts') LOOP
        INSERT INTO __forest_module_schema__.media_capture_parts(
            capture_id,part_id,part_index,part_kind,state,media_ref,locator
        ) VALUES (
            requested_capture,part->>'part_id',(part->>'part_index')::integer,'IMAGE','PENDING',part->>'media_ref',part->'locator'
        );
    END LOOP;
    result_value := jsonb_build_object('capture_id',requested_capture,'state','PENDING');
    RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',true,'value',result_value);
EXCEPTION WHEN unique_violation THEN
    SELECT * INTO existing FROM __forest_module_schema__.media_captures
    WHERE capture_id=requested_capture OR operation_id=requested_operation;
    IF FOUND AND existing.capture_id=requested_capture AND existing.operation_id=requested_operation
       AND (existing.input_payload-'observed_at')=(input-'observed_at') THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',true,'value',jsonb_build_object('capture_id',requested_capture,'state','PENDING'));
    END IF;
    RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,'error',jsonb_build_object('code','MEDIA_CAPTURE_CONFLICT','message','Media ingress identity was reused with another input','status',409));
WHEN invalid_text_representation OR datetime_field_overflow OR not_null_violation OR check_violation THEN
    RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,'error',jsonb_build_object('code','MEDIA_CAPTURE_INPUT_INVALID','message','Media ingress payload is invalid','status',422));
END;
$$;

CREATE FUNCTION media_claim_work(context jsonb, input jsonb)
RETURNS jsonb LANGUAGE plpgsql VOLATILE SET search_path = pg_catalog AS $$
DECLARE
    requested_token text := input->>'claim_token';
    requested_lease integer := (input->>'lease_seconds')::integer;
    capture __forest_module_schema__.media_captures%ROWTYPE;
    parts jsonb;
    payload jsonb;
BEGIN
    IF input->>'protocol' IS DISTINCT FROM 'forest.owned-work-claim-request/1' OR requested_token IS NULL OR requested_lease NOT BETWEEN 3 AND 86400 THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,'error',jsonb_build_object('code','MEDIA_CLAIM_INPUT_INVALID','message','Media work claim is invalid','status',422));
    END IF;
    UPDATE __forest_module_schema__.media_captures SET state='PENDING',claim_token=NULL,lease_deadline=NULL,next_attempt_at=clock_timestamp(),updated_at=clock_timestamp(),error_code='MEDIA_CAPTURE_LEASE_EXPIRED'
    WHERE state='CLAIMED' AND lease_deadline<=clock_timestamp();
    SELECT * INTO capture FROM __forest_module_schema__.media_captures
    WHERE state='PENDING' AND next_attempt_at<=clock_timestamp()
    ORDER BY next_attempt_at,capture_id FOR UPDATE SKIP LOCKED LIMIT 1;
    IF NOT FOUND THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',true,'value',NULL);
    END IF;
    UPDATE __forest_module_schema__.media_captures SET state='CLAIMED',attempt_count=attempt_count+1,claim_token=requested_token,lease_deadline=clock_timestamp()+(requested_lease*interval '1 second'),updated_at=clock_timestamp()
    WHERE capture_id=capture.capture_id RETURNING * INTO capture;
    SELECT COALESCE(jsonb_agg(jsonb_build_object(
        'part_id',p.part_id,'part_index',p.part_index,'part_kind',p.part_kind,'state',p.state,
        'media_ref',p.media_ref,'locator',p.locator,'payload',p.payload,'media_type',p.media_type,
        'width',p.width,'height',p.height,'byte_length',p.byte_length,'error_code',p.error_code
    ) ORDER BY p.part_index),'[]'::jsonb) INTO parts
    FROM __forest_module_schema__.media_capture_parts p WHERE p.capture_id=capture.capture_id;
    payload := jsonb_build_object(
        'capture_id',capture.capture_id,'claim_token',capture.claim_token,'identity',capture.identity,
        'source_unit_id',capture.source_unit_id,'source_revision',capture.source_revision,'unit_kind',capture.unit_kind,
        'author_subject_ref',capture.author_subject_ref,'author',capture.author,'binding_ref',capture.binding_ref,
        'conversation_ref',capture.conversation_ref,'event_ref',capture.event_ref,'ingress_seq',capture.ingress_seq,
        'observed_at',capture.observed_at,'occurred_at',capture.occurred_at,'parts',parts
    );
    RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',true,'value',jsonb_build_object(
        'protocol','forest.owned-work-claim/1','work_id',capture.capture_id,'claim_token',capture.claim_token,
        'attempt',capture.attempt_count,'operation_id','media-work:'||capture.capture_id||':'||capture.attempt_count,
        'causal_root_kind','media.capture','causal_root_id',capture.capture_id,'payload',payload
    ));
END;
$$;

CREATE FUNCTION media_capture_current(context jsonb, input jsonb)
RETURNS jsonb LANGUAGE plpgsql STABLE SET search_path = pg_catalog AS $$
DECLARE
    capture __forest_module_schema__.media_captures%ROWTYPE;
    parts jsonb;
BEGIN
    SELECT * INTO capture FROM __forest_module_schema__.media_captures WHERE capture_id=input->>'capture_id';
    IF NOT FOUND OR capture.state<>'CLAIMED' OR capture.claim_token IS DISTINCT FROM input->>'claim_token' THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,'error',jsonb_build_object('code','MEDIA_CAPTURE_CLAIM_LOST','message','Media capture claim is no longer current','status',409));
    END IF;
    SELECT COALESCE(jsonb_agg(jsonb_build_object('part_id',p.part_id,'part_index',p.part_index,'part_kind',p.part_kind,'state',p.state,'media_ref',p.media_ref,'locator',p.locator,'payload',p.payload,'media_type',p.media_type,'width',p.width,'height',p.height,'byte_length',p.byte_length,'error_code',p.error_code) ORDER BY p.part_index),'[]'::jsonb) INTO parts
    FROM __forest_module_schema__.media_capture_parts p WHERE p.capture_id=capture.capture_id;
    RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',true,'value',jsonb_build_object(
        'capture_id',capture.capture_id,'claim_token',capture.claim_token,'identity',capture.identity,'source_unit_id',capture.source_unit_id,
        'source_revision',capture.source_revision,'unit_kind',capture.unit_kind,'author_subject_ref',capture.author_subject_ref,
        'author',capture.author,'binding_ref',capture.binding_ref,'conversation_ref',capture.conversation_ref,'event_ref',capture.event_ref,
        'ingress_seq',capture.ingress_seq,'observed_at',capture.observed_at,'occurred_at',capture.occurred_at,'parts',parts
    ));
END;
$$;

CREATE FUNCTION media_publish_image(context jsonb, input jsonb)
RETURNS jsonb LANGUAGE plpgsql VOLATILE SET search_path = pg_catalog AS $$
DECLARE
    existing_input jsonb; existing_result jsonb; result_value jsonb;
    capture __forest_module_schema__.media_captures%ROWTYPE;
    part __forest_module_schema__.media_capture_parts%ROWTYPE;
    asset_count integer; asset_matches boolean;
BEGIN
    SELECT input_payload,result_payload INTO existing_input,existing_result FROM __forest_module_schema__.media_command_operations WHERE operation_id=context->>'operation_id';
    IF FOUND THEN
        IF existing_input=input THEN RETURN existing_result; END IF;
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,'error',jsonb_build_object('code','DOMAIN_OPERATION_CONFLICT','message','Media command operation was reused','status',409));
    END IF;
    SELECT * INTO capture FROM __forest_module_schema__.media_captures WHERE capture_id=input->>'capture_id' FOR UPDATE;
    SELECT * INTO part FROM __forest_module_schema__.media_capture_parts WHERE capture_id=input->>'capture_id' AND part_id=input->>'part_id' FOR UPDATE;
    IF NOT FOUND OR capture.state<>'CLAIMED' OR capture.claim_token IS DISTINCT FROM input->>'claim_token' OR part.media_ref IS DISTINCT FROM input->>'media_ref' THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,'error',jsonb_build_object('code','MEDIA_CAPTURE_CLAIM_LOST','message','Media capture claim is no longer current','status',409));
    END IF;
    INSERT INTO __forest_module_schema__.media_assets(media_ref,capture_id,part_id,payload,media_type,width,height,byte_length)
    VALUES(input->>'media_ref',input->>'capture_id',input->>'part_id',input->'payload',input->>'media_type',(input->>'width')::integer,(input->>'height')::integer,(input->>'byte_length')::integer)
    ON CONFLICT DO NOTHING;
    SELECT count(*),bool_and(
        a.media_ref=input->>'media_ref' AND a.capture_id=input->>'capture_id'
        AND a.part_id=input->>'part_id' AND a.payload=input->'payload'
        AND a.media_type=input->>'media_type' AND a.width=(input->>'width')::integer
        AND a.height=(input->>'height')::integer AND a.byte_length=(input->>'byte_length')::integer
    ) INTO asset_count,asset_matches
    FROM __forest_module_schema__.media_assets a
    WHERE a.media_ref=input->>'media_ref'
       OR (a.capture_id=input->>'capture_id' AND a.part_id=input->>'part_id');
    IF asset_count<>1 OR NOT asset_matches THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,'error',jsonb_build_object('code','MEDIA_ASSET_CONFLICT','message','Media asset identity names different content','status',409));
    END IF;
    UPDATE __forest_module_schema__.media_capture_parts SET state='READY',payload=input->'payload',media_type=input->>'media_type',width=(input->>'width')::integer,height=(input->>'height')::integer,byte_length=(input->>'byte_length')::integer,error_code=NULL
    WHERE capture_id=input->>'capture_id' AND part_id=input->>'part_id';
    result_value := jsonb_build_object('protocol','forest.domain-result/1','ok',true,'value',jsonb_build_object('capture_id',input->>'capture_id','part_id',input->>'part_id','state','READY'));
    INSERT INTO __forest_module_schema__.media_command_operations VALUES(context->>'operation_id','media.publish-image',input,result_value);
    RETURN result_value;
END;
$$;

CREATE FUNCTION media_fail_part(context jsonb, input jsonb)
RETURNS jsonb LANGUAGE plpgsql VOLATILE SET search_path = pg_catalog AS $$
DECLARE
    existing_input jsonb; existing_result jsonb; result_value jsonb; next_state text;
    capture __forest_module_schema__.media_captures%ROWTYPE;
BEGIN
    SELECT input_payload,result_payload INTO existing_input,existing_result FROM __forest_module_schema__.media_command_operations WHERE operation_id=context->>'operation_id';
    IF FOUND THEN
        IF existing_input=input THEN RETURN existing_result; END IF;
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,'error',jsonb_build_object('code','DOMAIN_OPERATION_CONFLICT','message','Media command operation was reused','status',409));
    END IF;
    SELECT * INTO capture FROM __forest_module_schema__.media_captures WHERE capture_id=input->>'capture_id' FOR UPDATE;
    IF NOT FOUND OR capture.state<>'CLAIMED' OR capture.claim_token IS DISTINCT FROM input->>'claim_token' THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,'error',jsonb_build_object('code','MEDIA_CAPTURE_CLAIM_LOST','message','Media capture claim is no longer current','status',409));
    END IF;
    next_state := CASE WHEN (input->>'final')::boolean THEN 'FAILED' ELSE 'PENDING' END;
    UPDATE __forest_module_schema__.media_capture_parts SET state=next_state,error_code=input->>'error_code'
    WHERE capture_id=input->>'capture_id' AND part_id=input->>'part_id' AND state='PENDING';
    result_value := jsonb_build_object('protocol','forest.domain-result/1','ok',true,'value',jsonb_build_object('capture_id',input->>'capture_id','part_id',input->>'part_id','state',next_state));
    INSERT INTO __forest_module_schema__.media_command_operations VALUES(context->>'operation_id','media.fail-part',input,result_value);
    RETURN result_value;
END;
$$;

CREATE FUNCTION media_assets_resolve(context jsonb, input jsonb)
RETURNS jsonb LANGUAGE plpgsql STABLE SET search_path = pg_catalog AS $$
DECLARE assets jsonb;
BEGIN
    SELECT COALESCE(jsonb_agg(jsonb_build_object(
        'source_unit_id',item.value->>'source_unit_id','part_id',item.value->>'part_id','media_ref',a.media_ref,
        'media_revision',a.media_revision,'payload',a.payload,'media_type',a.media_type,'width',a.width,'height',a.height,'byte_length',a.byte_length
    ) ORDER BY item.ordinality),'[]'::jsonb) INTO assets
    FROM jsonb_array_elements(input->'items') WITH ORDINALITY AS item(value,ordinality)
    JOIN __forest_module_schema__.media_assets a
      ON a.media_ref=item.value->>'media_ref'
     AND a.media_revision=(item.value->>'media_revision')::bigint;
    RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',true,'value',jsonb_build_object('assets',assets));
END;
$$;

CREATE FUNCTION media_settle_work(context jsonb, input jsonb)
RETURNS jsonb LANGUAGE plpgsql VOLATILE SET search_path = pg_catalog AS $$
DECLARE
    existing_input jsonb; existing_result jsonb; result_value jsonb;
    capture __forest_module_schema__.media_captures%ROWTYPE;
    outcome jsonb := input->'outcome'; disposition text; terminal_state text;
BEGIN
    SELECT input_payload,result_payload INTO existing_input,existing_result FROM __forest_module_schema__.media_work_settlements WHERE claim_token=input->>'claim_token';
    IF FOUND THEN
        IF existing_input=input THEN RETURN existing_result; END IF;
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,'error',jsonb_build_object('code','MEDIA_SETTLEMENT_CONFLICT','message','Media claim was settled with another outcome','status',409));
    END IF;
    SELECT * INTO capture FROM __forest_module_schema__.media_captures WHERE capture_id=input->>'work_id' FOR UPDATE;
    IF NOT FOUND OR capture.state<>'CLAIMED' OR capture.claim_token IS DISTINCT FROM input->>'claim_token' OR capture.attempt_count<>(input->>'attempt')::integer THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,'error',jsonb_build_object('code','MEDIA_CAPTURE_CLAIM_LOST','message','Media capture claim is no longer current','status',409));
    END IF;
    disposition := outcome->'result'->>'disposition';
    IF outcome->>'status'='SUCCEEDED' AND disposition='RETRY' THEN
        UPDATE __forest_module_schema__.media_captures SET state='PENDING',claim_token=NULL,lease_deadline=NULL,next_attempt_at=clock_timestamp()+(LEAST(power(2,capture.attempt_count),300)*interval '1 second'),settlement=outcome,error_code='MEDIA_CAPTURE_RETRY',updated_at=clock_timestamp() WHERE capture_id=capture.capture_id;
    ELSE
        terminal_state := CASE
            WHEN outcome->>'status'='SUCCEEDED' AND disposition IN ('READY','PARTIAL','FAILED') THEN disposition
            WHEN COALESCE((outcome->'error'->>'unknown')::boolean,false) THEN 'UNKNOWN'
            ELSE 'FAILED' END;
        UPDATE __forest_module_schema__.media_captures SET state=terminal_state,claim_token=NULL,lease_deadline=NULL,settlement=outcome,error_code=CASE WHEN terminal_state IN ('READY','PARTIAL') THEN NULL ELSE COALESCE(outcome->'error'->>'code','MEDIA_CAPTURE_UNAVAILABLE') END,settled_at=clock_timestamp(),updated_at=clock_timestamp() WHERE capture_id=capture.capture_id;
    END IF;
    result_value := jsonb_build_object('protocol','forest.domain-result/1','ok',true,'value',jsonb_build_object('protocol','forest.owned-work-settled/1','work_id',input->>'work_id'));
    INSERT INTO __forest_module_schema__.media_work_settlements VALUES(input->>'claim_token',input,result_value);
    RETURN result_value;
END;
$$;
