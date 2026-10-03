CREATE TABLE package_import_requests (
    operation_id text PRIMARY KEY,
    actor_ref text NOT NULL,
    generation_id text NOT NULL,
    content_length integer NOT NULL CHECK (content_length BETWEEN 1 AND 33554432),
    archive_sha256 text NOT NULL CHECK (archive_sha256 ~ '^sha256:[a-f0-9]{64}$'),
    status text NOT NULL CHECK (status IN ('RESERVED','SUCCEEDED','REJECTED','INTERRUPTED')),
    package_digest text CHECK (package_digest ~ '^sha256:[a-f0-9]{64}$'),
    error_code text CHECK (error_code ~ '^[A-Z][A-Z0-9_]{0,127}$'),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    CHECK ((status = 'SUCCEEDED') = (package_digest IS NOT NULL)),
    CHECK ((status = 'REJECTED') = (error_code IS NOT NULL))
);

CREATE INDEX package_import_requests_page
    ON package_import_requests (created_at DESC, operation_id DESC);

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
    FROM (SELECT * FROM __forest_module_schema__.package_import_requests AS item
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
