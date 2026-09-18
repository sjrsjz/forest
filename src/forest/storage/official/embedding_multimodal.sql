CREATE TABLE embedding_provider_identity (
    bot_id text PRIMARY KEY REFERENCES forest_kernel.bot_identity(bot_id),
    initialization_operation_id text NOT NULL
        CHECK (octet_length(initialization_operation_id) BETWEEN 1 AND 256),
    input_digest text NOT NULL CHECK (input_digest ~ '^sha256:[a-f0-9]{64}$'),
    deployment_id text NOT NULL CHECK (octet_length(deployment_id) BETWEEN 1 AND 256),
    model_revision text NOT NULL CHECK (octet_length(model_revision) BETWEEN 1 AND 256),
    model text NOT NULL CHECK (octet_length(model) BETWEEN 1 AND 256),
    dimensions integer NOT NULL CHECK (dimensions BETWEEN 1 AND 4096),
    space_id text NOT NULL CHECK (octet_length(space_id) BETWEEN 1 AND 256),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (bot_id,initialization_operation_id),
    UNIQUE (bot_id,space_id)
);

CREATE TABLE embedding_transport_attempts (
    bot_id text NOT NULL REFERENCES forest_kernel.bot_identity(bot_id),
    attempt_id text NOT NULL CHECK (octet_length(attempt_id) BETWEEN 1 AND 256),
    request_id text NOT NULL CHECK (octet_length(request_id) BETWEEN 1 AND 256),
    provider_generation_id text NOT NULL
        CHECK (octet_length(provider_generation_id) BETWEEN 1 AND 256),
    configuration_revision bigint NOT NULL CHECK (configuration_revision > 0),
    space_id text NOT NULL CHECK (octet_length(space_id) BETWEEN 1 AND 256),
    purpose text NOT NULL CHECK (purpose IN ('document','query')),
    input_count integer NOT NULL CHECK (input_count BETWEEN 1 AND 32),
    request_payload_digest text NOT NULL CHECK (
        request_payload_digest ~ '^sha256:[a-f0-9]{64}$'
    ),
    request_payload_id text NOT NULL CHECK (octet_length(request_payload_id) BETWEEN 1 AND 256),
    state text NOT NULL DEFAULT 'PREPARED' CHECK (
        state IN ('PREPARED','SENT','RESPONDED','REJECTED','NOT_SENT','UNKNOWN')
    ),
    result_payload_id text,
    result_payload_digest text CHECK (
        result_payload_digest IS NULL
        OR result_payload_digest ~ '^sha256:[a-f0-9]{64}$'
    ),
    usage_json text CHECK (usage_json IS NULL OR octet_length(usage_json) <= 4096),
    error_code text CHECK (
        error_code IS NULL OR error_code ~ '^[A-Z][A-Z0-9_]{0,127}$'
    ),
    http_status integer CHECK (http_status IS NULL OR http_status BETWEEN 100 AND 599),
    prepared_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    sent_at timestamptz,
    settled_at timestamptz,
    PRIMARY KEY (bot_id,attempt_id),
    UNIQUE (bot_id,request_id),
    CHECK (
        (state='PREPARED' AND sent_at IS NULL AND settled_at IS NULL)
        OR (state='SENT' AND sent_at IS NOT NULL AND settled_at IS NULL)
        OR (state IN ('RESPONDED','REJECTED','UNKNOWN')
            AND sent_at IS NOT NULL AND settled_at IS NOT NULL)
        OR (state='NOT_SENT' AND settled_at IS NOT NULL)
    ),
    CHECK (
        (state='RESPONDED' AND result_payload_id IS NOT NULL
            AND result_payload_digest IS NOT NULL AND error_code IS NULL)
        OR (state<>'RESPONDED' AND error_code IS NOT NULL)
        OR state IN ('PREPARED','SENT')
    )
);

CREATE FUNCTION forest_embedding_identity_immutable() RETURNS trigger
LANGUAGE plpgsql SET search_path FROM CURRENT AS $$
BEGIN
    RAISE EXCEPTION 'Forest embedding provider identity is immutable' USING ERRCODE='55000';
END;
$$;

CREATE TRIGGER embedding_provider_identity_immutable
    BEFORE UPDATE OR DELETE ON embedding_provider_identity
    FOR EACH ROW EXECUTE FUNCTION forest_embedding_identity_immutable();
