CREATE TABLE vector_index_identity (
    bot_id text PRIMARY KEY REFERENCES forest_kernel.bot_identity(bot_id),
    initialization_operation_id text NOT NULL
        CHECK (octet_length(initialization_operation_id) BETWEEN 1 AND 256),
    input_digest text NOT NULL CHECK (input_digest ~ '^sha256:[a-f0-9]{64}$'),
    space_id text NOT NULL CHECK (octet_length(space_id) BETWEEN 1 AND 256),
    dimensions integer NOT NULL CHECK (dimensions BETWEEN 1 AND 4096),
    metric text NOT NULL CHECK (metric='COSINE'),
    collection_name text NOT NULL CHECK (collection_name ~ '^[a-z][a-z0-9_]{0,254}$'),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (bot_id,initialization_operation_id),
    UNIQUE (bot_id,space_id),
    UNIQUE (collection_name)
);

CREATE FUNCTION forest_vector_index_identity_immutable() RETURNS trigger
LANGUAGE plpgsql SET search_path FROM CURRENT AS $$
BEGIN
    RAISE EXCEPTION 'Forest vector index identity is immutable' USING ERRCODE='55000';
END;
$$;

CREATE TRIGGER vector_index_identity_immutable
    BEFORE UPDATE OR DELETE ON vector_index_identity
    FOR EACH ROW EXECUTE FUNCTION forest_vector_index_identity_immutable();
