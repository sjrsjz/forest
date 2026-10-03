CREATE FUNCTION deployment_accounts_metadata_page(context jsonb, input jsonb)
RETURNS jsonb LANGUAGE plpgsql STABLE SET search_path = pg_catalog AS $$
DECLARE
    requested_limit integer := COALESCE((input->>'limit')::integer,50);
    after_username text := input->>'after';
    search_text text := input->>'search';
    entries jsonb;
    found_count integer;
BEGIN
    IF context->>'scope_kind' IS DISTINCT FROM 'deployment'
       OR context->>'bot_id' IS NOT NULL
       OR context->>'component_id' IS DISTINCT FROM 'deployment.accounts'
       OR context->>'port_name' IS DISTINCT FROM 'deployment.accounts.metadata.page'
       OR context->>'port_kind' IS DISTINCT FROM 'query'
       OR jsonb_typeof(input) IS DISTINCT FROM 'object'
       OR input - ARRAY['limit','after','search']::text[] != '{}'::jsonb
       OR requested_limit NOT BETWEEN 1 AND 100
       OR (after_username IS NOT NULL AND octet_length(after_username) NOT BETWEEN 1 AND 64)
       OR (search_text IS NOT NULL AND octet_length(search_text)>128) THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object('code','WEB_ACCOUNT_QUERY_INVALID',
                'message','Account query is invalid','status',422));
    END IF;
    SELECT COALESCE(jsonb_agg(jsonb_build_object(
        'subject_id',page.subject_id,'username',page.username,
        'display_name',page.display_name,'role',page.role,
        'enabled',page.enabled,'revision',page.revision)
        ORDER BY page.username),'[]'::jsonb),count(*)
    INTO entries,found_count
    FROM (
        SELECT subject_id,username,display_name,role,enabled,revision
        FROM forest_deployment.web_accounts
        WHERE (after_username IS NULL OR username>after_username)
          AND (search_text IS NULL OR search_text='' OR
               username ILIKE '%'||search_text||'%' OR
               display_name ILIKE '%'||search_text||'%')
        ORDER BY username LIMIT requested_limit+1
    ) page;
    IF found_count>requested_limit THEN
        entries:=entries-requested_limit;
    END IF;
    RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',true,
        'value',jsonb_build_object('items',entries,
            'next_after',CASE WHEN found_count>requested_limit
                THEN entries->(requested_limit-1)->>'username' ELSE NULL END));
END;
$$;
