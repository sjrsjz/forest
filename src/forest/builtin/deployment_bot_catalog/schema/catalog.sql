CREATE FUNCTION deployment_bot_catalog_page(context jsonb, input jsonb)
RETURNS jsonb LANGUAGE plpgsql STABLE SET search_path = pg_catalog AS $$
BEGIN
    IF context->>'scope_kind' IS DISTINCT FROM 'deployment'
       OR context->>'bot_id' IS NOT NULL
       OR context->>'component_id' IS DISTINCT FROM 'deployment.bot-catalog'
       OR context->>'port_name' IS DISTINCT FROM 'deployment.bot-catalog.page'
       OR context->>'port_kind' IS DISTINCT FROM 'query'
       OR jsonb_typeof(input) IS DISTINCT FROM 'object'
       OR input - ARRAY['limit','after','search']::text[] != '{}'::jsonb THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object('code','BOT_CATALOG_QUERY_INVALID',
                'message','Bot catalog query is invalid','status',422));
    END IF;
    RETURN forest_deployment.bot_catalog_page(
        COALESCE((input->>'limit')::integer,50),input->>'after',input->>'search');
END;
$$;

CREATE FUNCTION deployment_bot_catalog_set_desired(context jsonb, input jsonb)
RETURNS jsonb LANGUAGE plpgsql VOLATILE SET search_path = pg_catalog AS $$
BEGIN
    IF context->>'scope_kind' IS DISTINCT FROM 'deployment'
       OR context->>'bot_id' IS NOT NULL
       OR context->>'component_id' IS DISTINCT FROM 'deployment.bot-catalog'
       OR context->>'port_name' IS DISTINCT FROM 'deployment.bot-catalog.set-desired'
       OR context->>'port_kind' IS DISTINCT FROM 'management_action'
       OR jsonb_typeof(input) IS DISTINCT FROM 'object'
       OR input - ARRAY['bot_id','desired_state','expected_revision']::text[] != '{}'::jsonb THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object('code','DEPLOYMENT_INPUT_INVALID',
                'message','Runtime intent is invalid','status',422));
    END IF;
    RETURN forest_deployment.bot_set_desired(
        context->>'operation_id',context->>'input_digest',input->>'bot_id',
        input->>'desired_state',(input->>'expected_revision')::bigint,
        context->>'component_id',context->>'generation_id');
END;
$$;
