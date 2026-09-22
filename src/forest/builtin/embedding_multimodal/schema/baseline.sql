CREATE TABLE embedding_provider_identity (
    bot_id text PRIMARY KEY,
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
