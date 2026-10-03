CREATE FUNCTION deployment_provisioning_jobs_page(context jsonb, input jsonb)
RETURNS jsonb LANGUAGE plpgsql STABLE SET search_path = pg_catalog AS $$
DECLARE
    requested_limit integer := COALESCE((input->>'limit')::integer,50);
    after_job text := input->>'after';
    requested_bot text := input->>'bot_id';
    requested_state text := input->>'state';
    entries jsonb;
    found_count integer;
BEGIN
    IF context->>'scope_kind' IS DISTINCT FROM 'deployment'
       OR context->>'bot_id' IS NOT NULL
       OR context->>'component_id' IS DISTINCT FROM 'deployment.provisioning'
       OR context->>'port_name' IS DISTINCT FROM 'deployment.provisioning.jobs.read'
       OR context->>'port_kind' IS DISTINCT FROM 'query'
       OR jsonb_typeof(input) IS DISTINCT FROM 'object'
       OR input - ARRAY['limit','after','bot_id','state']::text[] != '{}'::jsonb
       OR requested_limit NOT BETWEEN 1 AND 100
       OR (after_job IS NOT NULL AND octet_length(after_job) NOT BETWEEN 1 AND 256)
       OR (requested_bot IS NOT NULL AND octet_length(requested_bot) NOT BETWEEN 1 AND 128)
       OR (requested_state IS NOT NULL AND requested_state NOT IN
           ('QUEUED','RUNNING','RETRY_WAIT','SUCCEEDED','FAILED','CANCELLED')) THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object('code','PROVISIONING_QUERY_INVALID',
                'message','Provisioning query is invalid','status',422));
    END IF;
    SELECT COALESCE(jsonb_agg(jsonb_build_object(
        'job_id',page.job_id,'bot_id',page.bot_id,'state',page.state,
        'revision',page.revision,'attempt_no',page.attempt_no,
        'max_attempts',page.max_attempts,'last_error_code',page.last_error_code)
        ORDER BY page.job_id),'[]'::jsonb),count(*)
    INTO entries,found_count
    FROM (
        SELECT job_id,bot_id,state,revision,attempt_no,max_attempts,last_error_code
        FROM forest_deployment.provisioning_jobs
        WHERE (after_job IS NULL OR job_id>after_job)
          AND (requested_bot IS NULL OR bot_id=requested_bot)
          AND (requested_state IS NULL OR state=requested_state)
        ORDER BY job_id LIMIT requested_limit+1
    ) page;
    IF found_count>requested_limit THEN
        entries:=entries-requested_limit;
    END IF;
    RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',true,
        'value',jsonb_build_object('items',entries,
            'next_after',CASE WHEN found_count>requested_limit
                THEN entries->(requested_limit-1)->>'job_id' ELSE NULL END));
END;
$$;
