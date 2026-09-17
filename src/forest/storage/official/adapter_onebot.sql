CREATE TABLE binding_heads (
    bot_id text NOT NULL REFERENCES forest_kernel.bot_identity(bot_id),
    binding_id text NOT NULL CHECK (octet_length(binding_id) BETWEEN 1 AND 256),
    platform text NOT NULL DEFAULT 'onebot-v11' CHECK (platform = 'onebot-v11'),
    self_id text NOT NULL CHECK (self_id ~ '^[1-9][0-9]{0,18}$'),
    registry_operation_id text NOT NULL
        CHECK (octet_length(registry_operation_id) BETWEEN 1 AND 256),
    registry_input_digest text NOT NULL CHECK (
        registry_input_digest ~ '^sha256:[a-f0-9]{64}$'
    ),
    display_payload_id text NOT NULL CHECK (
        octet_length(display_payload_id) BETWEEN 1 AND 256
    ),
    display_payload_digest text NOT NULL CHECK (
        display_payload_digest ~ '^sha256:[a-f0-9]{64}$'
    ),
    display_owner_component_id text NOT NULL CHECK (
        octet_length(display_owner_component_id) BETWEEN 1 AND 128
    ),
    display_owner_kind text NOT NULL CHECK (
        octet_length(display_owner_kind) BETWEEN 1 AND 128
    ),
    display_owner_id text NOT NULL CHECK (
        octet_length(display_owner_id) BETWEEN 1 AND 512
    ),
    desired_revision bigint NOT NULL DEFAULT 0 CHECK (desired_revision >= 0),
    effective_revision bigint NOT NULL DEFAULT 0 CHECK (
        effective_revision BETWEEN 0 AND desired_revision
    ),
    binding_epoch bigint NOT NULL DEFAULT 0 CHECK (binding_epoch >= 0),
    connection_epoch bigint NOT NULL DEFAULT 0 CHECK (connection_epoch >= 0),
    state text NOT NULL DEFAULT 'DISABLED' CHECK (
        state IN ('DISABLED','OFFLINE','ONLINE','DRAINING','FAILED')
    ),
    desired_enabled boolean NOT NULL DEFAULT false,
    effective_enabled boolean NOT NULL DEFAULT false,
    receive_enabled boolean NOT NULL DEFAULT false,
    current_client_incarnation text,
    error_code text CHECK (error_code IS NULL OR octet_length(error_code) BETWEEN 1 AND 128),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (bot_id,binding_id),
    UNIQUE (bot_id,platform,self_id),
    UNIQUE (bot_id,registry_operation_id),
    CHECK (
        (state IN ('ONLINE','DRAINING') AND current_client_incarnation IS NOT NULL
            AND effective_enabled)
        OR (state IN ('DISABLED','OFFLINE','FAILED') AND current_client_incarnation IS NULL)
    ),
    CHECK (receive_enabled = false OR state = 'ONLINE'),
    CHECK (state <> 'DISABLED' OR effective_enabled = false)
);

CREATE TABLE binding_config_plans (
    bot_id text NOT NULL,
    plan_id text NOT NULL CHECK (octet_length(plan_id) BETWEEN 1 AND 256),
    input_digest text NOT NULL CHECK (input_digest ~ '^sha256:[a-f0-9]{64}$'),
    binding_id text NOT NULL,
    expected_revision bigint NOT NULL CHECK (expected_revision >= 0),
    enabled boolean NOT NULL,
    receive_enabled boolean NOT NULL,
    transport text NOT NULL CHECK (transport IN ('HTTP_POST','FORWARD_WEBSOCKET')),
    config_payload_id text NOT NULL CHECK (
        octet_length(config_payload_id) BETWEEN 1 AND 256
    ),
    config_payload_digest text NOT NULL CHECK (
        config_payload_digest ~ '^sha256:[a-f0-9]{64}$'
    ),
    config_owner_component_id text NOT NULL CHECK (
        octet_length(config_owner_component_id) BETWEEN 1 AND 128
    ),
    config_owner_kind text NOT NULL CHECK (
        octet_length(config_owner_kind) BETWEEN 1 AND 128
    ),
    config_owner_id text NOT NULL CHECK (
        octet_length(config_owner_id) BETWEEN 1 AND 512
    ),
    secret_ref text NOT NULL CHECK (octet_length(secret_ref) BETWEEN 1 AND 512),
    secret_digest text NOT NULL CHECK (secret_digest ~ '^sha256:[a-f0-9]{64}$'),
    actor_principal_ref text NOT NULL CHECK (
        octet_length(actor_principal_ref) BETWEEN 1 AND 256
    ),
    expires_at timestamptz NOT NULL,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (bot_id,plan_id),
    FOREIGN KEY (bot_id,binding_id) REFERENCES binding_heads(bot_id,binding_id),
    CHECK (receive_enabled = false OR enabled),
    CHECK (expires_at > created_at AND expires_at <= created_at + interval '15 minutes')
);

CREATE TABLE binding_revisions (
    bot_id text NOT NULL,
    binding_id text NOT NULL,
    revision bigint NOT NULL CHECK (revision > 0),
    previous_revision bigint NOT NULL CHECK (previous_revision = revision - 1),
    plan_id text NOT NULL CHECK (octet_length(plan_id) BETWEEN 1 AND 256),
    operation_id text NOT NULL CHECK (octet_length(operation_id) BETWEEN 1 AND 256),
    input_digest text NOT NULL CHECK (input_digest ~ '^sha256:[a-f0-9]{64}$'),
    enabled boolean NOT NULL,
    receive_enabled boolean NOT NULL,
    transport text NOT NULL CHECK (transport IN ('HTTP_POST','FORWARD_WEBSOCKET')),
    config_payload_id text NOT NULL CHECK (
        octet_length(config_payload_id) BETWEEN 1 AND 256
    ),
    config_payload_digest text NOT NULL CHECK (
        config_payload_digest ~ '^sha256:[a-f0-9]{64}$'
    ),
    config_owner_component_id text NOT NULL CHECK (
        octet_length(config_owner_component_id) BETWEEN 1 AND 128
    ),
    config_owner_kind text NOT NULL CHECK (
        octet_length(config_owner_kind) BETWEEN 1 AND 128
    ),
    config_owner_id text NOT NULL CHECK (
        octet_length(config_owner_id) BETWEEN 1 AND 512
    ),
    secret_ref text NOT NULL CHECK (octet_length(secret_ref) BETWEEN 1 AND 512),
    secret_digest text NOT NULL CHECK (secret_digest ~ '^sha256:[a-f0-9]{64}$'),
    actor_principal_ref text NOT NULL CHECK (
        octet_length(actor_principal_ref) BETWEEN 1 AND 256
    ),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (bot_id,binding_id,revision),
    UNIQUE (bot_id,operation_id),
    FOREIGN KEY (bot_id,binding_id) REFERENCES binding_heads(bot_id,binding_id),
    FOREIGN KEY (bot_id,plan_id) REFERENCES binding_config_plans(bot_id,plan_id),
    CHECK (receive_enabled = false OR enabled)
);

CREATE TABLE binding_clients (
    bot_id text NOT NULL,
    client_incarnation text NOT NULL CHECK (
        octet_length(client_incarnation) BETWEEN 1 AND 256
    ),
    binding_id text NOT NULL,
    revision bigint NOT NULL CHECK (revision > 0),
    expected_binding_epoch bigint NOT NULL CHECK (expected_binding_epoch >= 0),
    adapter_generation_id text NOT NULL CHECK (
        octet_length(adapter_generation_id) BETWEEN 1 AND 256
    ),
    executor_fence_digest text NOT NULL CHECK (
        executor_fence_digest ~ '^sha256:[a-f0-9]{64}$'
    ),
    transport text NOT NULL CHECK (transport IN ('HTTP_POST','FORWARD_WEBSOCKET')),
    state text NOT NULL DEFAULT 'BUFFERING' CHECK (
        state IN ('BUFFERING','ACTIVE','RETIRING','CLOSED','FAILED')
    ),
    published_binding_epoch bigint,
    connection_epoch bigint,
    error_code text CHECK (error_code IS NULL OR octet_length(error_code) BETWEEN 1 AND 128),
    prepared_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    activated_at timestamptz,
    retired_at timestamptz,
    closed_at timestamptz,
    PRIMARY KEY (bot_id,client_incarnation),
    FOREIGN KEY (bot_id,binding_id,revision)
        REFERENCES binding_revisions(bot_id,binding_id,revision),
    UNIQUE (bot_id,binding_id,published_binding_epoch),
    UNIQUE (bot_id,binding_id,connection_epoch),
    CHECK ((published_binding_epoch IS NULL) = (connection_epoch IS NULL)),
    CHECK (
        (state = 'BUFFERING' AND published_binding_epoch IS NULL
            AND activated_at IS NULL AND retired_at IS NULL AND closed_at IS NULL
            AND error_code IS NULL)
        OR (state = 'ACTIVE' AND published_binding_epoch IS NOT NULL
            AND activated_at IS NOT NULL AND retired_at IS NULL AND closed_at IS NULL
            AND error_code IS NULL)
        OR (state = 'RETIRING' AND published_binding_epoch IS NOT NULL
            AND activated_at IS NOT NULL AND retired_at IS NOT NULL AND closed_at IS NULL
            AND error_code IS NULL)
        OR (state = 'CLOSED' AND closed_at IS NOT NULL AND error_code IS NULL)
        OR (state = 'FAILED' AND closed_at IS NOT NULL AND error_code IS NOT NULL)
    )
);

ALTER TABLE binding_heads ADD CONSTRAINT binding_current_client_fk
    FOREIGN KEY (bot_id,current_client_incarnation)
    REFERENCES binding_clients(bot_id,client_incarnation)
    DEFERRABLE INITIALLY DEFERRED;

CREATE TABLE binding_publications (
    bot_id text NOT NULL,
    operation_id text NOT NULL CHECK (octet_length(operation_id) BETWEEN 1 AND 256),
    input_digest text NOT NULL CHECK (input_digest ~ '^sha256:[a-f0-9]{64}$'),
    binding_id text NOT NULL,
    revision bigint NOT NULL CHECK (revision > 0),
    publication_kind text NOT NULL CHECK (publication_kind IN ('CLIENT','DISABLED')),
    client_incarnation text,
    previous_binding_epoch bigint NOT NULL CHECK (previous_binding_epoch >= 0),
    resulting_binding_epoch bigint NOT NULL CHECK (
        resulting_binding_epoch = previous_binding_epoch + 1
    ),
    resulting_connection_epoch bigint NOT NULL CHECK (resulting_connection_epoch >= 0),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (bot_id,operation_id),
    FOREIGN KEY (bot_id,binding_id,revision)
        REFERENCES binding_revisions(bot_id,binding_id,revision),
    FOREIGN KEY (bot_id,client_incarnation)
        REFERENCES binding_clients(bot_id,client_incarnation),
    CHECK (
        (publication_kind = 'CLIENT' AND client_incarnation IS NOT NULL)
        OR (publication_kind = 'DISABLED' AND client_incarnation IS NULL)
    )
);

CREATE TABLE binding_client_observations (
    bot_id text NOT NULL,
    operation_id text NOT NULL CHECK (octet_length(operation_id) BETWEEN 1 AND 256),
    input_digest text NOT NULL CHECK (input_digest ~ '^sha256:[a-f0-9]{64}$'),
    client_incarnation text NOT NULL,
    observed_state text NOT NULL CHECK (
        observed_state IN ('RETIRING','CLOSED','FAILED')
    ),
    error_code text CHECK (error_code IS NULL OR octet_length(error_code) BETWEEN 1 AND 128),
    accepted boolean NOT NULL,
    previous_state text NOT NULL CHECK (
        previous_state IN ('BUFFERING','ACTIVE','RETIRING','CLOSED','FAILED')
    ),
    resulting_state text NOT NULL CHECK (
        resulting_state IN ('BUFFERING','ACTIVE','RETIRING','CLOSED','FAILED')
    ),
    conflict_code text,
    observed_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (bot_id,operation_id),
    FOREIGN KEY (bot_id,client_incarnation)
        REFERENCES binding_clients(bot_id,client_incarnation),
    CHECK ((observed_state = 'FAILED') = (error_code IS NOT NULL)),
    CHECK ((accepted AND conflict_code IS NULL) OR (NOT accepted AND conflict_code IS NOT NULL))
);

CREATE TABLE platform_events (
    bot_id text NOT NULL,
    event_id text NOT NULL CHECK (octet_length(event_id) BETWEEN 1 AND 256),
    semantic_digest text NOT NULL CHECK (semantic_digest ~ '^sha256:[a-f0-9]{64}$'),
    binding_id text NOT NULL,
    binding_epoch bigint NOT NULL CHECK (binding_epoch > 0),
    client_incarnation text NOT NULL,
    platform_message_ref text NOT NULL CHECK (
        octet_length(platform_message_ref) BETWEEN 1 AND 512
    ),
    event_kind text NOT NULL CHECK (octet_length(event_kind) BETWEEN 1 AND 128),
    channel_kind text NOT NULL CHECK (channel_kind IN ('GROUP','PRIVATE')),
    channel_ref text NOT NULL CHECK (octet_length(channel_ref) BETWEEN 1 AND 256),
    conversation_ref text NOT NULL CHECK (
        octet_length(conversation_ref) BETWEEN 1 AND 256
    ),
    author_subject_ref text NOT NULL CHECK (
        octet_length(author_subject_ref) BETWEEN 1 AND 256
    ),
    raw_payload_id text NOT NULL CHECK (octet_length(raw_payload_id) BETWEEN 1 AND 256),
    raw_payload_digest text NOT NULL CHECK (raw_payload_digest ~ '^sha256:[a-f0-9]{64}$'),
    raw_owner_component_id text NOT NULL CHECK (
        octet_length(raw_owner_component_id) BETWEEN 1 AND 128
    ),
    raw_owner_kind text NOT NULL CHECK (octet_length(raw_owner_kind) BETWEEN 1 AND 128),
    raw_owner_id text NOT NULL CHECK (octet_length(raw_owner_id) BETWEEN 1 AND 512),
    normalized_payload_id text NOT NULL CHECK (
        octet_length(normalized_payload_id) BETWEEN 1 AND 256
    ),
    normalized_payload_digest text NOT NULL CHECK (
        normalized_payload_digest ~ '^sha256:[a-f0-9]{64}$'
    ),
    normalized_owner_component_id text NOT NULL CHECK (
        octet_length(normalized_owner_component_id) BETWEEN 1 AND 128
    ),
    normalized_owner_kind text NOT NULL CHECK (
        octet_length(normalized_owner_kind) BETWEEN 1 AND 128
    ),
    normalized_owner_id text NOT NULL CHECK (
        octet_length(normalized_owner_id) BETWEEN 1 AND 512
    ),
    author_payload_id text NOT NULL CHECK (
        octet_length(author_payload_id) BETWEEN 1 AND 256
    ),
    author_payload_digest text NOT NULL CHECK (
        author_payload_digest ~ '^sha256:[a-f0-9]{64}$'
    ),
    author_owner_component_id text NOT NULL CHECK (
        octet_length(author_owner_component_id) BETWEEN 1 AND 128
    ),
    author_owner_kind text NOT NULL CHECK (
        octet_length(author_owner_kind) BETWEEN 1 AND 128
    ),
    author_owner_id text NOT NULL CHECK (
        octet_length(author_owner_id) BETWEEN 1 AND 512
    ),
    sent_at timestamptz,
    received_at timestamptz NOT NULL,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (bot_id,event_id),
    UNIQUE (bot_id,binding_id,platform_message_ref),
    FOREIGN KEY (bot_id,binding_id) REFERENCES binding_heads(bot_id,binding_id),
    FOREIGN KEY (bot_id,client_incarnation)
        REFERENCES binding_clients(bot_id,client_incarnation)
);

CREATE TABLE event_admission_operations (
    bot_id text NOT NULL,
    operation_id text NOT NULL CHECK (octet_length(operation_id) BETWEEN 1 AND 256),
    input_digest text NOT NULL CHECK (input_digest ~ '^sha256:[a-f0-9]{64}$'),
    event_id text NOT NULL,
    duplicate boolean NOT NULL,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (bot_id,operation_id),
    FOREIGN KEY (bot_id,event_id) REFERENCES platform_events(bot_id,event_id)
);

CREATE INDEX binding_heads_state_idx ON binding_heads(bot_id,state,binding_id);
CREATE INDEX binding_clients_state_idx ON binding_clients(bot_id,state,prepared_at);
CREATE INDEX platform_events_conversation_idx ON platform_events(
    bot_id,conversation_ref,received_at,event_id
);

CREATE FUNCTION reject_immutable_row() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    RAISE EXCEPTION 'adapter.onebot immutable fact cannot change' USING ERRCODE='55000';
END;
$$;

CREATE FUNCTION guard_binding_head() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF ROW(NEW.bot_id,NEW.binding_id,NEW.platform,NEW.self_id,NEW.registry_operation_id,
           NEW.registry_input_digest,NEW.display_payload_id,NEW.display_payload_digest,
           NEW.display_owner_component_id,NEW.display_owner_kind,NEW.display_owner_id,
           NEW.created_at)
       IS DISTINCT FROM
       ROW(OLD.bot_id,OLD.binding_id,OLD.platform,OLD.self_id,OLD.registry_operation_id,
           OLD.registry_input_digest,OLD.display_payload_id,OLD.display_payload_digest,
           OLD.display_owner_component_id,OLD.display_owner_kind,OLD.display_owner_id,
           OLD.created_at) THEN
        RAISE EXCEPTION 'Binding identity is immutable' USING ERRCODE='55000';
    END IF;
    IF NEW.desired_revision < OLD.desired_revision OR
       NEW.effective_revision < OLD.effective_revision OR
       NEW.binding_epoch < OLD.binding_epoch OR
       NEW.connection_epoch < OLD.connection_epoch THEN
        RAISE EXCEPTION 'Binding head cannot move backwards' USING ERRCODE='55000';
    END IF;
    RETURN NEW;
END;
$$;

CREATE FUNCTION guard_binding_client() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF ROW(NEW.bot_id,NEW.client_incarnation,NEW.binding_id,NEW.revision,
           NEW.expected_binding_epoch,NEW.adapter_generation_id,
           NEW.executor_fence_digest,NEW.transport,NEW.prepared_at)
       IS DISTINCT FROM
       ROW(OLD.bot_id,OLD.client_incarnation,OLD.binding_id,OLD.revision,
           OLD.expected_binding_epoch,OLD.adapter_generation_id,
           OLD.executor_fence_digest,OLD.transport,OLD.prepared_at) THEN
        RAISE EXCEPTION 'Client identity is immutable' USING ERRCODE='55000';
    END IF;
    IF OLD.state = 'BUFFERING' AND NEW.state IN ('ACTIVE','CLOSED','FAILED') THEN
        RETURN NEW;
    END IF;
    IF OLD.state = 'ACTIVE' AND NEW.state IN ('RETIRING','CLOSED','FAILED') THEN
        RETURN NEW;
    END IF;
    IF OLD.state = 'RETIRING' AND NEW.state IN ('CLOSED','FAILED') THEN
        RETURN NEW;
    END IF;
    IF NEW IS NOT DISTINCT FROM OLD THEN
        RETURN NEW;
    END IF;
    RAISE EXCEPTION 'Invalid client state transition' USING ERRCODE='55000';
END;
$$;

CREATE TRIGGER binding_head_guard BEFORE UPDATE OR DELETE ON binding_heads
    FOR EACH ROW EXECUTE FUNCTION guard_binding_head();
CREATE TRIGGER binding_client_guard BEFORE UPDATE OR DELETE ON binding_clients
    FOR EACH ROW EXECUTE FUNCTION guard_binding_client();
CREATE TRIGGER binding_revisions_immutable BEFORE UPDATE OR DELETE ON binding_revisions
    FOR EACH ROW EXECUTE FUNCTION reject_immutable_row();
CREATE TRIGGER binding_config_plans_immutable BEFORE UPDATE OR DELETE ON binding_config_plans
    FOR EACH ROW EXECUTE FUNCTION reject_immutable_row();
CREATE TRIGGER binding_publications_immutable BEFORE UPDATE OR DELETE ON binding_publications
    FOR EACH ROW EXECUTE FUNCTION reject_immutable_row();
CREATE TRIGGER binding_client_observations_immutable
    BEFORE UPDATE OR DELETE ON binding_client_observations
    FOR EACH ROW EXECUTE FUNCTION reject_immutable_row();
CREATE TRIGGER platform_events_immutable BEFORE UPDATE OR DELETE ON platform_events
    FOR EACH ROW EXECUTE FUNCTION reject_immutable_row();
CREATE TRIGGER event_admission_operations_immutable
    BEFORE UPDATE OR DELETE ON event_admission_operations
    FOR EACH ROW EXECUTE FUNCTION reject_immutable_row();
