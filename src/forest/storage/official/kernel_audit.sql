CREATE TABLE records (
    bot_id text NOT NULL REFERENCES forest_kernel.bot_identity(bot_id),
    audit_id text NOT NULL CHECK (
        octet_length(audit_id) BETWEEN 1 AND 128
        AND audit_id ~ '^[A-Za-z0-9][A-Za-z0-9_.:-]*$'
    ),
    action text NOT NULL CHECK (
        octet_length(action) BETWEEN 1 AND 128
        AND action ~ '^[A-Za-z0-9][A-Za-z0-9_.:-]*$'
    ),
    severity text NOT NULL CHECK (severity IN ('INFO', 'WARNING', 'ERROR', 'SECURITY')),
    producer_component_id text NOT NULL CHECK (
        octet_length(producer_component_id) BETWEEN 1 AND 128
        AND producer_component_id ~ '^[A-Za-z0-9][A-Za-z0-9_.:-]*$'
    ),
    producer_generation_id text,
    origin_kind text NOT NULL CHECK (
        octet_length(origin_kind) BETWEEN 1 AND 64
        AND origin_kind ~ '^[A-Za-z0-9][A-Za-z0-9_.:-]*$'
    ),
    actor_kind text,
    actor_id text,
    resource_kind text,
    resource_id text,
    operation_kind text,
    operation_id text,
    call_id text,
    details_payload_id text,
    details_payload_digest text CHECK (
        details_payload_digest IS NULL
        OR details_payload_digest ~ '^sha256:[a-f0-9]{64}$'
    ),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (bot_id, audit_id),
    CHECK ((actor_kind IS NULL) = (actor_id IS NULL)),
    CHECK ((resource_kind IS NULL) = (resource_id IS NULL)),
    CHECK ((operation_kind IS NULL) = (operation_id IS NULL)),
    CHECK ((details_payload_id IS NULL) = (details_payload_digest IS NULL)),
    CHECK (
        actor_kind IS NULL OR (
            octet_length(actor_kind) BETWEEN 1 AND 64
            AND actor_kind ~ '^[A-Za-z0-9][A-Za-z0-9_.:-]*$'
        )
    ),
    CHECK (actor_id IS NULL OR octet_length(actor_id) BETWEEN 1 AND 256),
    CHECK (
        resource_kind IS NULL OR (
            octet_length(resource_kind) BETWEEN 1 AND 64
            AND resource_kind ~ '^[A-Za-z0-9][A-Za-z0-9_.:-]*$'
        )
    ),
    CHECK (resource_id IS NULL OR octet_length(resource_id) BETWEEN 1 AND 512),
    CHECK (
        operation_kind IS NULL OR (
            octet_length(operation_kind) BETWEEN 1 AND 64
            AND operation_kind ~ '^[A-Za-z0-9][A-Za-z0-9_.:-]*$'
        )
    ),
    CHECK (operation_id IS NULL OR octet_length(operation_id) BETWEEN 1 AND 256),
    CHECK (call_id IS NULL OR octet_length(call_id) BETWEEN 1 AND 256),
    CHECK (
        producer_generation_id IS NULL
        OR octet_length(producer_generation_id) BETWEEN 1 AND 256
    )
);

CREATE INDEX records_time_idx
    ON records(bot_id, created_at DESC, audit_id DESC);
CREATE INDEX records_action_time_idx
    ON records(bot_id, action, created_at DESC, audit_id DESC);
CREATE INDEX records_producer_time_idx
    ON records(bot_id, producer_component_id, created_at DESC, audit_id DESC);
CREATE INDEX records_resource_time_idx
    ON records(bot_id, resource_kind, resource_id, created_at DESC, audit_id DESC)
    WHERE resource_kind IS NOT NULL;
CREATE INDEX records_operation_idx
    ON records(bot_id, operation_kind, operation_id)
    WHERE operation_kind IS NOT NULL;

CREATE FUNCTION reject_record_mutation() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    RAISE EXCEPTION 'Forest audit facts are immutable' USING ERRCODE = '55000';
END;
$$;

CREATE TRIGGER records_immutable BEFORE UPDATE OR DELETE ON records
    FOR EACH ROW EXECUTE FUNCTION reject_record_mutation();
