CREATE FUNCTION deployment_secret_metadata_page(context jsonb, input jsonb)
RETURNS jsonb LANGUAGE plpgsql STABLE SET search_path = pg_catalog AS $$
BEGIN
    IF context->>'scope_kind' IS DISTINCT FROM 'deployment'
       OR context->>'bot_id' IS NOT NULL
       OR context->>'component_id' IS DISTINCT FROM 'deployment.secrets'
       OR context->>'port_name' IS DISTINCT FROM 'deployment.secrets.metadata.page'
       OR context->>'port_kind' IS DISTINCT FROM 'query'
       OR jsonb_typeof(input) IS DISTINCT FROM 'object'
       OR input - ARRAY['limit','after','search']::text[] != '{}'::jsonb THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object('code','SECRET_QUERY_INVALID',
                'message','Secret metadata query is invalid','status',422));
    END IF;
    RETURN forest_deployment.secret_metadata_page(
        COALESCE((input->>'limit')::integer,50),input->>'after',input->>'search');
END;
$$;
