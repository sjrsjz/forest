CREATE FUNCTION host_audit_page(context jsonb, input jsonb)
RETURNS jsonb LANGUAGE plpgsql STABLE SET search_path = pg_catalog AS $$
DECLARE
    host text := context->>'scope_id';
    page_limit integer := COALESCE((input->>'limit')::integer, 50);
    cursor_time timestamptz;
    cursor_id text;
    items jsonb;
    total integer;
    next_before text;
BEGIN
    IF context->>'scope_kind' IS DISTINCT FROM 'host'
       OR context->>'bot_id' IS NOT NULL
       OR context->>'component_id' IS DISTINCT FROM 'host.audit'
       OR context->>'port_name' IS DISTINCT FROM 'host.audit.page'
       OR host IS NULL
       OR jsonb_typeof(input) IS DISTINCT FROM 'object'
       OR input - ARRAY['action','producer_component_id','origin_kind','actor_kind',
                        'actor_id','resource_kind','resource_id','operation_kind',
                        'operation_id','before','limit']::text[] != '{}'::jsonb
       OR page_limit NOT BETWEEN 1 AND 100
       OR ((input ? 'actor_kind') <> (input ? 'actor_id'))
       OR ((input ? 'resource_kind') <> (input ? 'resource_id')) THEN
        RETURN jsonb_build_object(
            'protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object('code','HOST_AUDIT_QUERY_INVALID',
                'message','Audit query identity or filters are invalid','status',422));
    END IF;

    IF input ? 'before' THEN
        SELECT created_at,audit_id INTO cursor_time,cursor_id
        FROM forest_host.audit_records
        WHERE host_id=host AND audit_id=input->>'before';
        IF NOT FOUND THEN
            RETURN jsonb_build_object(
                'protocol','forest.domain-result/1','ok',false,
                'error',jsonb_build_object('code','HOST_AUDIT_CURSOR_NOT_FOUND',
                    'message','Audit cursor is unavailable','status',404));
        END IF;
    END IF;

    SELECT COALESCE(jsonb_agg(jsonb_build_object(
        'audit_id',page.audit_id,
        'action',page.action,
        'severity',page.severity,
        'producer_component_id',page.producer_component_id,
        'producer_generation_id',page.producer_generation_id,
        'origin_kind',page.origin_kind,
        'actor_kind',page.actor_kind,
        'actor_id',page.actor_id,
        'resource_kind',page.resource_kind,
        'resource_id',page.resource_id,
        'operation_kind',page.operation_kind,
        'operation_id',page.operation_id,
        'call_id',NULL,
        'details_payload_id',NULL,
        'details_payload_digest',NULL,
        'created_at',page.created_at
    ) ORDER BY page.created_at DESC,page.audit_id DESC),'[]'::jsonb), COUNT(*)
    INTO items,total
    FROM (
        SELECT * FROM forest_host.audit_records AS records
        WHERE records.host_id=host
          AND (input->>'action' IS NULL OR records.action=input->>'action')
          AND (input->>'producer_component_id' IS NULL OR
               records.producer_component_id=input->>'producer_component_id')
          AND (input->>'origin_kind' IS NULL OR records.origin_kind=input->>'origin_kind')
          AND (input->>'actor_kind' IS NULL OR records.actor_kind=input->>'actor_kind')
          AND (input->>'actor_id' IS NULL OR records.actor_id=input->>'actor_id')
          AND (input->>'resource_kind' IS NULL OR records.resource_kind=input->>'resource_kind')
          AND (input->>'resource_id' IS NULL OR records.resource_id=input->>'resource_id')
          AND (input->>'operation_kind' IS NULL OR records.operation_kind=input->>'operation_kind')
          AND (input->>'operation_id' IS NULL OR records.operation_id=input->>'operation_id')
          AND (cursor_id IS NULL OR (records.created_at,records.audit_id)<(cursor_time,cursor_id))
        ORDER BY records.created_at DESC,records.audit_id DESC
        LIMIT page_limit+1
    ) AS page;

    IF total > page_limit THEN
        items := items - page_limit;
        next_before := items->(page_limit-1)->>'audit_id';
    END IF;
    RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',true,
        'value',jsonb_build_object('records',items,'next_before',next_before));
END;
$$;
