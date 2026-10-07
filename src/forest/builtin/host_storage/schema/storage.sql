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
