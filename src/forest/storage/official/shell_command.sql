CREATE TABLE policy_heads (
    bot_id text NOT NULL REFERENCES forest_kernel.bot_identity(bot_id),
    binding_ref text NOT NULL CHECK (octet_length(binding_ref) BETWEEN 1 AND 256),
    revision bigint NOT NULL DEFAULT 0 CHECK (revision >= 0),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (bot_id,binding_ref)
);

CREATE TABLE policy_revisions (
    bot_id text NOT NULL,
    binding_ref text NOT NULL,
    revision bigint NOT NULL CHECK (revision > 0),
    previous_revision bigint NOT NULL CHECK (previous_revision = revision - 1),
    operation_id text NOT NULL CHECK (octet_length(operation_id) BETWEEN 1 AND 256),
    input_digest text NOT NULL CHECK (input_digest ~ '^sha256:[a-f0-9]{64}$'),
    actor_principal_ref text NOT NULL
        CHECK (octet_length(actor_principal_ref) BETWEEN 1 AND 256),
    grant_count integer NOT NULL CHECK (grant_count BETWEEN 0 AND 4096),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (bot_id,binding_ref,revision),
    UNIQUE (bot_id,operation_id),
    FOREIGN KEY (bot_id,binding_ref) REFERENCES policy_heads(bot_id,binding_ref)
);

CREATE TABLE policy_grants (
    bot_id text NOT NULL,
    binding_ref text NOT NULL,
    revision bigint NOT NULL,
    grant_index integer NOT NULL CHECK (grant_index >= 0),
    scope_kind text NOT NULL CHECK (scope_kind IN ('BINDING','CONVERSATION')),
    conversation_ref text CHECK (
        conversation_ref IS NULL OR octet_length(conversation_ref) BETWEEN 1 AND 256
    ),
    principal_ref text NOT NULL CHECK (octet_length(principal_ref) BETWEEN 1 AND 256),
    allow_p0 boolean NOT NULL,
    PRIMARY KEY (bot_id,binding_ref,revision,grant_index),
    UNIQUE NULLS NOT DISTINCT (
        bot_id,binding_ref,revision,scope_kind,conversation_ref,principal_ref
    ),
    FOREIGN KEY (bot_id,binding_ref,revision)
        REFERENCES policy_revisions(bot_id,binding_ref,revision),
    CHECK (
        (scope_kind='BINDING' AND conversation_ref IS NULL)
        OR (scope_kind='CONVERSATION' AND conversation_ref IS NOT NULL)
    )
);

CREATE TABLE registry_heads (
    bot_id text PRIMARY KEY REFERENCES forest_kernel.bot_identity(bot_id),
    revision bigint NOT NULL DEFAULT 0 CHECK (revision >= 0),
    graph_digest text CHECK (graph_digest IS NULL OR graph_digest ~ '^sha256:[a-f0-9]{64}$'),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    CHECK ((revision=0) = (graph_digest IS NULL))
);

CREATE TABLE registry_revisions (
    bot_id text NOT NULL,
    revision bigint NOT NULL CHECK (revision > 0),
    previous_revision bigint NOT NULL CHECK (previous_revision = revision - 1),
    operation_id text NOT NULL CHECK (octet_length(operation_id) BETWEEN 1 AND 256),
    input_digest text NOT NULL CHECK (input_digest ~ '^sha256:[a-f0-9]{64}$'),
    graph_digest text NOT NULL CHECK (graph_digest ~ '^sha256:[a-f0-9]{64}$'),
    route_count integer NOT NULL CHECK (route_count BETWEEN 1 AND 4096),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (bot_id,revision),
    UNIQUE (bot_id,operation_id),
    FOREIGN KEY (bot_id) REFERENCES registry_heads(bot_id)
);

CREATE TABLE command_routes (
    bot_id text NOT NULL,
    registry_revision bigint NOT NULL,
    command_id text NOT NULL CHECK (octet_length(command_id) BETWEEN 1 AND 128),
    command_version integer NOT NULL CHECK (command_version > 0),
    owner_component_id text NOT NULL
        CHECK (octet_length(owner_component_id) BETWEEN 1 AND 128),
    owner_generation_id text NOT NULL
        CHECK (octet_length(owner_generation_id) BETWEEN 1 AND 256),
    argument_schema_digest text NOT NULL CHECK (
        argument_schema_digest ~ '^sha256:[a-f0-9]{64}$'
    ),
    management_operation_id text NOT NULL CHECK (
        octet_length(management_operation_id) BETWEEN 1 AND 256
    ),
    target_scope text NOT NULL CHECK (
        target_scope IN ('CONVERSATION','BINDING','BOT','DEPLOYMENT')
    ),
    service_class text NOT NULL CHECK (service_class IN ('P0','P1')),
    required_capabilities_digest text NOT NULL CHECK (
        required_capabilities_digest ~ '^sha256:[a-f0-9]{64}$'
    ),
    output_policy text NOT NULL CHECK (output_policy IN ('SHORT','ARTIFACT','SILENT')),
    timeout_seconds integer NOT NULL CHECK (timeout_seconds BETWEEN 1 AND 900),
    route_digest text NOT NULL CHECK (route_digest ~ '^sha256:[a-f0-9]{64}$'),
    PRIMARY KEY (bot_id,registry_revision,command_id),
    FOREIGN KEY (bot_id,registry_revision)
        REFERENCES registry_revisions(bot_id,revision)
);

CREATE TABLE shell_requests (
    bot_id text NOT NULL REFERENCES forest_kernel.bot_identity(bot_id),
    request_id text NOT NULL CHECK (octet_length(request_id) BETWEEN 1 AND 256),
    admission_operation_id text NOT NULL
        CHECK (octet_length(admission_operation_id) BETWEEN 1 AND 256),
    input_digest text NOT NULL CHECK (input_digest ~ '^sha256:[a-f0-9]{64}$'),
    event_ref text NOT NULL CHECK (octet_length(event_ref) BETWEEN 1 AND 512),
    conversation_ref text NOT NULL
        CHECK (octet_length(conversation_ref) BETWEEN 1 AND 256),
    binding_ref text NOT NULL CHECK (octet_length(binding_ref) BETWEEN 1 AND 256),
    binding_epoch bigint NOT NULL CHECK (binding_epoch > 0),
    connection_epoch bigint NOT NULL CHECK (connection_epoch > 0),
    client_incarnation text NOT NULL
        CHECK (octet_length(client_incarnation) BETWEEN 1 AND 256),
    adapter_generation_id text NOT NULL
        CHECK (octet_length(adapter_generation_id) BETWEEN 1 AND 256),
    executor_fence_digest text NOT NULL CHECK (
        executor_fence_digest ~ '^sha256:[a-f0-9]{64}$'
    ),
    principal_ref text NOT NULL CHECK (octet_length(principal_ref) BETWEEN 1 AND 256),
    gate_proof_digest text NOT NULL CHECK (gate_proof_digest ~ '^sha256:[a-f0-9]{64}$'),
    command_line_digest text NOT NULL CHECK (
        command_line_digest ~ '^sha256:[a-f0-9]{64}$'
    ),
    command_id text NOT NULL CHECK (octet_length(command_id) BETWEEN 1 AND 128),
    command_version integer NOT NULL CHECK (command_version > 0),
    policy_revision bigint NOT NULL CHECK (policy_revision >= 0),
    registry_revision bigint NOT NULL CHECK (registry_revision >= 0),
    graph_digest text CHECK (graph_digest IS NULL OR graph_digest ~ '^sha256:[a-f0-9]{64}$'),
    owner_component_id text CHECK (
        owner_component_id IS NULL OR octet_length(owner_component_id) BETWEEN 1 AND 128
    ),
    owner_generation_id text CHECK (
        owner_generation_id IS NULL OR octet_length(owner_generation_id) BETWEEN 1 AND 256
    ),
    argument_schema_digest text CHECK (
        argument_schema_digest IS NULL OR argument_schema_digest ~ '^sha256:[a-f0-9]{64}$'
    ),
    management_operation_id text CHECK (
        management_operation_id IS NULL
        OR octet_length(management_operation_id) BETWEEN 1 AND 256
    ),
    target_scope text CHECK (
        target_scope IS NULL OR target_scope IN ('CONVERSATION','BINDING','BOT','DEPLOYMENT')
    ),
    requested_service_class text CHECK (
        requested_service_class IS NULL OR requested_service_class IN ('P0','P1')
    ),
    effective_service_class text NOT NULL CHECK (effective_service_class IN ('P0','P1')),
    required_capabilities_digest text CHECK (
        required_capabilities_digest IS NULL
        OR required_capabilities_digest ~ '^sha256:[a-f0-9]{64}$'
    ),
    output_policy text CHECK (
        output_policy IS NULL OR output_policy IN ('SHORT','ARTIFACT','SILENT')
    ),
    timeout_seconds integer CHECK (
        timeout_seconds IS NULL OR timeout_seconds BETWEEN 1 AND 900
    ),
    route_digest text CHECK (route_digest IS NULL OR route_digest ~ '^sha256:[a-f0-9]{64}$'),
    arguments_payload_id text NOT NULL
        CHECK (octet_length(arguments_payload_id) BETWEEN 1 AND 256),
    arguments_payload_digest text NOT NULL CHECK (
        arguments_payload_digest ~ '^sha256:[a-f0-9]{64}$'
    ),
    arguments_owner_component_id text NOT NULL CHECK (
        octet_length(arguments_owner_component_id) BETWEEN 1 AND 128
    ),
    arguments_owner_kind text NOT NULL
        CHECK (octet_length(arguments_owner_kind) BETWEEN 1 AND 128),
    arguments_owner_id text NOT NULL CHECK (
        octet_length(arguments_owner_id) BETWEEN 1 AND 512
    ),
    idempotency_key text NOT NULL CHECK (octet_length(idempotency_key) BETWEEN 1 AND 256),
    state text NOT NULL CHECK (
        state IN ('QUEUED','CLAIMED','EXECUTING','WAITING_APPROVAL','REJECTED',
                  'SUCCEEDED','FAILED','CANCELLED','UNKNOWN')
    ),
    rejection_code text CHECK (
        rejection_code IS NULL OR octet_length(rejection_code) BETWEEN 1 AND 128
    ),
    owner_id text CHECK (owner_id IS NULL OR octet_length(owner_id) BETWEEN 1 AND 256),
    owner_epoch bigint NOT NULL DEFAULT 0 CHECK (owner_epoch >= 0),
    owner_expires_at timestamptz,
    received_at timestamptz NOT NULL,
    admission_deadline timestamptz NOT NULL,
    started_at timestamptz,
    completed_at timestamptz,
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (bot_id,request_id),
    UNIQUE (bot_id,admission_operation_id),
    UNIQUE (bot_id,event_ref),
    UNIQUE (bot_id,idempotency_key),
    CHECK (admission_deadline > received_at),
    CHECK (
        (state='REJECTED' AND rejection_code IS NOT NULL AND owner_id IS NULL
            AND owner_expires_at IS NULL AND completed_at IS NOT NULL)
        OR (state='QUEUED' AND rejection_code IS NULL AND owner_id IS NULL
            AND owner_expires_at IS NULL AND completed_at IS NULL)
        OR (state IN ('CLAIMED','EXECUTING','WAITING_APPROVAL') AND rejection_code IS NULL
            AND owner_id IS NOT NULL AND owner_epoch > 0 AND owner_expires_at IS NOT NULL
            AND completed_at IS NULL)
        OR (state IN ('SUCCEEDED','FAILED','CANCELLED','UNKNOWN') AND rejection_code IS NULL
            AND owner_id IS NULL AND owner_expires_at IS NULL AND completed_at IS NOT NULL)
    ),
    CHECK (
        (registry_revision=0 AND graph_digest IS NULL AND owner_component_id IS NULL
            AND owner_generation_id IS NULL AND argument_schema_digest IS NULL
            AND management_operation_id IS NULL AND target_scope IS NULL
            AND requested_service_class IS NULL AND required_capabilities_digest IS NULL
            AND output_policy IS NULL AND timeout_seconds IS NULL AND route_digest IS NULL
            AND state='REJECTED' AND rejection_code='SHELL_COMMAND_NOT_FOUND')
        OR (registry_revision>0 AND graph_digest IS NOT NULL AND owner_component_id IS NOT NULL
            AND owner_generation_id IS NOT NULL AND argument_schema_digest IS NOT NULL
            AND management_operation_id IS NOT NULL AND target_scope IS NOT NULL
            AND requested_service_class IS NOT NULL AND required_capabilities_digest IS NOT NULL
            AND output_policy IS NOT NULL AND timeout_seconds IS NOT NULL
            AND route_digest IS NOT NULL)
    )
);

CREATE TABLE claim_operations (
    bot_id text NOT NULL,
    operation_id text NOT NULL CHECK (octet_length(operation_id) BETWEEN 1 AND 256),
    input_digest text NOT NULL CHECK (input_digest ~ '^sha256:[a-f0-9]{64}$'),
    request_id text NOT NULL,
    owner_id text NOT NULL CHECK (octet_length(owner_id) BETWEEN 1 AND 256),
    owner_epoch bigint NOT NULL CHECK (owner_epoch > 0),
    owner_expires_at timestamptz NOT NULL,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (bot_id,operation_id),
    FOREIGN KEY (bot_id,request_id) REFERENCES shell_requests(bot_id,request_id)
);

CREATE TABLE request_results (
    bot_id text NOT NULL,
    operation_id text NOT NULL CHECK (octet_length(operation_id) BETWEEN 1 AND 256),
    input_digest text NOT NULL CHECK (input_digest ~ '^sha256:[a-f0-9]{64}$'),
    request_id text NOT NULL,
    resulting_state text NOT NULL CHECK (
        resulting_state IN ('SUCCEEDED','FAILED','CANCELLED','UNKNOWN')
    ),
    result_payload_id text NOT NULL CHECK (octet_length(result_payload_id) BETWEEN 1 AND 256),
    result_payload_digest text NOT NULL CHECK (
        result_payload_digest ~ '^sha256:[a-f0-9]{64}$'
    ),
    result_owner_component_id text NOT NULL CHECK (
        octet_length(result_owner_component_id) BETWEEN 1 AND 128
    ),
    result_owner_kind text NOT NULL CHECK (
        octet_length(result_owner_kind) BETWEEN 1 AND 128
    ),
    result_owner_id text NOT NULL CHECK (octet_length(result_owner_id) BETWEEN 1 AND 512),
    error_code text CHECK (error_code IS NULL OR octet_length(error_code) BETWEEN 1 AND 128),
    owner_id text NOT NULL CHECK (octet_length(owner_id) BETWEEN 1 AND 256),
    owner_epoch bigint NOT NULL CHECK (owner_epoch > 0),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (bot_id,operation_id),
    UNIQUE (bot_id,request_id),
    FOREIGN KEY (bot_id,request_id) REFERENCES shell_requests(bot_id,request_id),
    CHECK ((resulting_state='SUCCEEDED') = (error_code IS NULL))
);

CREATE INDEX shell_requests_queue_idx ON shell_requests(
    bot_id,state,effective_service_class,received_at,request_id
);
CREATE INDEX shell_requests_conversation_idx ON shell_requests(
    bot_id,conversation_ref,received_at,request_id
);

CREATE FUNCTION reject_immutable_row() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    RAISE EXCEPTION 'shell.command immutable fact cannot change' USING ERRCODE='55000';
END;
$$;

CREATE FUNCTION guard_policy_head() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF ROW(NEW.bot_id,NEW.binding_ref) IS DISTINCT FROM ROW(OLD.bot_id,OLD.binding_ref)
       OR NEW.revision < OLD.revision THEN
        RAISE EXCEPTION 'Shell policy head cannot move backwards' USING ERRCODE='55000';
    END IF;
    RETURN NEW;
END;
$$;

CREATE FUNCTION guard_registry_head() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF NEW.bot_id IS DISTINCT FROM OLD.bot_id OR NEW.revision < OLD.revision THEN
        RAISE EXCEPTION 'Shell registry head cannot move backwards' USING ERRCODE='55000';
    END IF;
    RETURN NEW;
END;
$$;

CREATE FUNCTION guard_shell_request() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF ROW(NEW.bot_id,NEW.request_id,NEW.admission_operation_id,NEW.input_digest,
           NEW.event_ref,NEW.conversation_ref,NEW.binding_ref,NEW.binding_epoch,
           NEW.connection_epoch,NEW.client_incarnation,NEW.adapter_generation_id,
           NEW.executor_fence_digest,NEW.principal_ref,NEW.gate_proof_digest,
           NEW.command_line_digest,NEW.command_id,NEW.command_version,NEW.policy_revision,
           NEW.registry_revision,NEW.graph_digest,NEW.owner_component_id,
           NEW.owner_generation_id,NEW.argument_schema_digest,NEW.management_operation_id,
           NEW.target_scope,NEW.requested_service_class,NEW.effective_service_class,
           NEW.required_capabilities_digest,NEW.output_policy,NEW.timeout_seconds,
           NEW.route_digest,NEW.arguments_payload_id,NEW.arguments_payload_digest,
           NEW.arguments_owner_component_id,NEW.arguments_owner_kind,NEW.arguments_owner_id,
           NEW.idempotency_key,NEW.received_at,NEW.admission_deadline)
       IS DISTINCT FROM
       ROW(OLD.bot_id,OLD.request_id,OLD.admission_operation_id,OLD.input_digest,
           OLD.event_ref,OLD.conversation_ref,OLD.binding_ref,OLD.binding_epoch,
           OLD.connection_epoch,OLD.client_incarnation,OLD.adapter_generation_id,
           OLD.executor_fence_digest,OLD.principal_ref,OLD.gate_proof_digest,
           OLD.command_line_digest,OLD.command_id,OLD.command_version,OLD.policy_revision,
           OLD.registry_revision,OLD.graph_digest,OLD.owner_component_id,
           OLD.owner_generation_id,OLD.argument_schema_digest,OLD.management_operation_id,
           OLD.target_scope,OLD.requested_service_class,OLD.effective_service_class,
           OLD.required_capabilities_digest,OLD.output_policy,OLD.timeout_seconds,
           OLD.route_digest,OLD.arguments_payload_id,OLD.arguments_payload_digest,
           OLD.arguments_owner_component_id,OLD.arguments_owner_kind,OLD.arguments_owner_id,
           OLD.idempotency_key,OLD.received_at,OLD.admission_deadline) THEN
        RAISE EXCEPTION 'Shell request identity is immutable' USING ERRCODE='55000';
    END IF;
    IF OLD.state='QUEUED' AND NEW.state IN ('CLAIMED','REJECTED','CANCELLED') THEN RETURN NEW; END IF;
    IF OLD.state='CLAIMED' AND NEW.state IN (
        'QUEUED','EXECUTING','SUCCEEDED','FAILED','CANCELLED','UNKNOWN'
    ) THEN RETURN NEW; END IF;
    IF OLD.state='EXECUTING' AND NEW.state IN (
        'WAITING_APPROVAL','SUCCEEDED','FAILED','CANCELLED','UNKNOWN'
    ) THEN RETURN NEW; END IF;
    IF OLD.state='WAITING_APPROVAL' AND NEW.state IN (
        'EXECUTING','SUCCEEDED','FAILED','CANCELLED','UNKNOWN'
    ) THEN RETURN NEW; END IF;
    IF NEW IS NOT DISTINCT FROM OLD THEN RETURN NEW; END IF;
    RAISE EXCEPTION 'Invalid Shell request transition' USING ERRCODE='55000';
END;
$$;

CREATE TRIGGER policy_head_guard BEFORE UPDATE OR DELETE ON policy_heads
    FOR EACH ROW EXECUTE FUNCTION guard_policy_head();
CREATE TRIGGER registry_head_guard BEFORE UPDATE OR DELETE ON registry_heads
    FOR EACH ROW EXECUTE FUNCTION guard_registry_head();
CREATE TRIGGER shell_request_guard BEFORE UPDATE OR DELETE ON shell_requests
    FOR EACH ROW EXECUTE FUNCTION guard_shell_request();
CREATE TRIGGER policy_revisions_immutable BEFORE UPDATE OR DELETE ON policy_revisions
    FOR EACH ROW EXECUTE FUNCTION reject_immutable_row();
CREATE TRIGGER policy_grants_immutable BEFORE UPDATE OR DELETE ON policy_grants
    FOR EACH ROW EXECUTE FUNCTION reject_immutable_row();
CREATE TRIGGER registry_revisions_immutable BEFORE UPDATE OR DELETE ON registry_revisions
    FOR EACH ROW EXECUTE FUNCTION reject_immutable_row();
CREATE TRIGGER command_routes_immutable BEFORE UPDATE OR DELETE ON command_routes
    FOR EACH ROW EXECUTE FUNCTION reject_immutable_row();
CREATE TRIGGER claim_operations_immutable BEFORE UPDATE OR DELETE ON claim_operations
    FOR EACH ROW EXECUTE FUNCTION reject_immutable_row();
CREATE TRIGGER request_results_immutable BEFORE UPDATE OR DELETE ON request_results
    FOR EACH ROW EXECUTE FUNCTION reject_immutable_row();
