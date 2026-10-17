CREATE TABLE runtime_state (
    bot_id text PRIMARY KEY REFERENCES forest_kernel.bot_identity(bot_id),
    storage_generation bigint NOT NULL DEFAULT 1 CHECK (storage_generation > 0),
    status text NOT NULL DEFAULT 'ACTIVE' CHECK (status IN ('ACTIVE', 'SEALED')),
    maintenance_epoch bigint NOT NULL DEFAULT 1 CHECK (maintenance_epoch > 0),
    policy_revision bigint NOT NULL DEFAULT 1 CHECK (policy_revision > 0),
    data_admission boolean NOT NULL DEFAULT true,
    control_admission boolean NOT NULL DEFAULT true
);

CREATE TABLE rescue_policy_state (
    bot_id text PRIMARY KEY REFERENCES forest_kernel.bot_identity(bot_id),
    revision bigint NOT NULL DEFAULT 1 CHECK (revision > 0)
);

CREATE TABLE rescue_grants (
    bot_id text NOT NULL REFERENCES rescue_policy_state(bot_id),
    binding_ref text NOT NULL CHECK (octet_length(binding_ref) BETWEEN 1 AND 256),
    group_ref text NOT NULL CHECK (group_ref ~ '^[1-9][0-9]{0,19}$'),
    principal_ref text NOT NULL CHECK (principal_ref ~ '^qq:[1-9][0-9]{0,19}$'),
    allow_status boolean NOT NULL,
    allow_cancel boolean NOT NULL,
    allow_disable boolean NOT NULL,
    allow_reload boolean NOT NULL,
    changed_revision bigint NOT NULL CHECK (changed_revision > 1),
    PRIMARY KEY (bot_id,binding_ref,group_ref,principal_ref),
    CHECK (allow_status OR allow_cancel OR allow_disable OR allow_reload)
);

CREATE TABLE rescue_policy_operations (
    bot_id text NOT NULL REFERENCES rescue_policy_state(bot_id),
    operation_id text NOT NULL CHECK (octet_length(operation_id) BETWEEN 1 AND 256),
    input_digest text NOT NULL CHECK (input_digest ~ '^sha256:[a-f0-9]{64}$'),
    actor_ref text NOT NULL CHECK (octet_length(actor_ref) BETWEEN 1 AND 256),
    resulting_revision bigint NOT NULL CHECK (resulting_revision > 1),
    changed boolean NOT NULL,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (bot_id,operation_id)
);

CREATE TABLE rescue_requests (
    bot_id text NOT NULL REFERENCES rescue_policy_state(bot_id),
    request_id text NOT NULL CHECK (octet_length(request_id) BETWEEN 1 AND 256),
    input_digest text NOT NULL CHECK (input_digest ~ '^sha256:[a-f0-9]{64}$'),
    event_ref text NOT NULL CHECK (octet_length(event_ref) BETWEEN 1 AND 256),
    conversation_ref text NOT NULL CHECK (octet_length(conversation_ref) BETWEEN 1 AND 256),
    binding_ref text NOT NULL CHECK (octet_length(binding_ref) BETWEEN 1 AND 256),
    binding_epoch bigint NOT NULL CHECK (binding_epoch > 0),
    self_id text NOT NULL CHECK (self_id ~ '^[1-9][0-9]{0,18}$'),
    connection_epoch bigint NOT NULL CHECK (connection_epoch > 0),
    client_incarnation text NOT NULL CHECK (octet_length(client_incarnation) BETWEEN 1 AND 256),
    adapter_generation_id text NOT NULL CHECK (octet_length(adapter_generation_id) BETWEEN 1 AND 256),
    executor_fence_digest text NOT NULL CHECK (executor_fence_digest ~ '^sha256:[a-f0-9]{64}$'),
    group_ref text NOT NULL CHECK (group_ref ~ '^[1-9][0-9]{0,19}$'),
    principal_ref text NOT NULL CHECK (octet_length(principal_ref) BETWEEN 1 AND 256),
    ingress_seq bigint NOT NULL CHECK (ingress_seq > 0),
    policy_revision bigint NOT NULL CHECK (policy_revision > 0),
    kernel_generation_id text NOT NULL CHECK (octet_length(kernel_generation_id) BETWEEN 1 AND 256),
    action text CHECK (action IN ('status','cancel','disable','reload')),
    target_ref text,
    expected_revision bigint CHECK (expected_revision > 0),
    service_class text CHECK (service_class IN ('P0','P1')),
    admission_state text NOT NULL CHECK (admission_state IN ('QUEUED','REJECTED')),
    state text NOT NULL CHECK (state IN ('QUEUED','REJECTED')),
    rejection_code text,
    received_at timestamptz NOT NULL,
    admission_deadline timestamptz NOT NULL,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (bot_id,request_id),
    UNIQUE (bot_id,event_ref),
    CHECK (
        (admission_state = 'QUEUED' AND action IS NOT NULL
            AND service_class IS NOT NULL AND rejection_code IS NULL)
        OR (admission_state = 'REJECTED' AND rejection_code IN (
            'CORE_RESCUE_SYNTAX_INVALID','CORE_RESCUE_PRINCIPAL_DENIED',
            'CORE_RESCUE_QUEUE_FULL'
        ))
    )
);

CREATE INDEX rescue_requests_queue_idx
    ON rescue_requests(bot_id,state,created_at,request_id);

CREATE TABLE host_lease (
    bot_id text PRIMARY KEY REFERENCES forest_kernel.bot_identity(bot_id),
    host_id text,
    incarnation text,
    owner_epoch bigint NOT NULL DEFAULT 0 CHECK (owner_epoch >= 0),
    expires_at timestamptz NOT NULL DEFAULT '1970-01-01 UTC',
    CHECK ((host_id IS NULL) = (incarnation IS NULL))
);

CREATE TABLE host_incarnations (
    bot_id text NOT NULL REFERENCES forest_kernel.bot_identity(bot_id),
    incarnation text NOT NULL,
    host_id text NOT NULL,
    owner_epoch bigint NOT NULL CHECK (owner_epoch > 0),
    PRIMARY KEY (bot_id, incarnation),
    UNIQUE (bot_id, owner_epoch),
    UNIQUE (bot_id, incarnation, owner_epoch)
);

CREATE TABLE module_resources (
    bot_id text NOT NULL REFERENCES forest_kernel.bot_identity(bot_id),
    component_id text NOT NULL,
    operation_id text,
    operation_epoch bigint NOT NULL DEFAULT 0 CHECK (operation_epoch >= 0),
    lease_until timestamptz NOT NULL DEFAULT '1970-01-01 UTC',
    PRIMARY KEY (bot_id, component_id)
);

CREATE TABLE generations (
    bot_id text NOT NULL REFERENCES forest_kernel.bot_identity(bot_id),
    generation_id text NOT NULL,
    component_id text NOT NULL,
    package_digest text NOT NULL CHECK (package_digest ~ '^sha256:[a-f0-9]{64}$'),
    config_revision bigint NOT NULL CHECK (config_revision > 0),
    config_payload_id text NOT NULL,
    config_payload_digest text NOT NULL CHECK (config_payload_digest ~ '^sha256:[a-f0-9]{64}$'),
    instance_id text NOT NULL,
    worker_incarnation text NOT NULL,
    host_incarnation text NOT NULL,
    host_owner_epoch bigint NOT NULL CHECK (host_owner_epoch > 0),
    state_schema integer NOT NULL DEFAULT 1 CHECK (state_schema > 0),
    state_contract_digest text NOT NULL CHECK (state_contract_digest ~ '^sha256:[a-f0-9]{64}$'),
    capability_grants_digest text NOT NULL CHECK (capability_grants_digest ~ '^sha256:[a-f0-9]{64}$'),
    status text NOT NULL CHECK (status IN (
        'PREPARING', 'READY', 'ACTIVE', 'QUIESCING', 'QUIESCENT',
        'RETIRED', 'FAILED', 'DISPOSED'
    )),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (bot_id, generation_id),
    UNIQUE (bot_id, generation_id, component_id),
    UNIQUE (bot_id, generation_id, component_id, package_digest),
    UNIQUE (bot_id, worker_incarnation),
    FOREIGN KEY (bot_id, host_incarnation, host_owner_epoch)
        REFERENCES host_incarnations(bot_id, incarnation, owner_epoch)
);

CREATE TABLE routes (
    bot_id text NOT NULL REFERENCES forest_kernel.bot_identity(bot_id),
    component_id text NOT NULL,
    generation_id text,
    route_epoch bigint NOT NULL DEFAULT 0 CHECK (route_epoch >= 0),
    data_admission boolean NOT NULL DEFAULT false,
    control_admission boolean NOT NULL DEFAULT false,
    PRIMARY KEY (bot_id, component_id),
    FOREIGN KEY (bot_id, generation_id, component_id)
        REFERENCES generations(bot_id, generation_id, component_id)
);

CREATE TABLE lifecycle_operations (
    bot_id text NOT NULL REFERENCES forest_kernel.bot_identity(bot_id),
    operation_id text NOT NULL,
    component_id text NOT NULL,
    operation_kind text NOT NULL CHECK (operation_kind IN ('INSTALL', 'RELOAD', 'DISABLE')),
    input_digest text NOT NULL CHECK (input_digest ~ '^sha256:[a-f0-9]{64}$'),
    candidate_generation_id text,
    old_generation_id text,
    expected_route_epoch bigint NOT NULL CHECK (expected_route_epoch >= 0),
    phase text NOT NULL CHECK (phase IN (
        'PLAN', 'FROZEN', 'QUIESCENT', 'COMMITTED', 'FAILED', 'ABANDONED'
    )),
    host_incarnation text NOT NULL,
    host_owner_epoch bigint NOT NULL CHECK (host_owner_epoch > 0),
    operation_epoch bigint NOT NULL CHECK (operation_epoch > 0),
    lease_until timestamptz NOT NULL,
    error_code text,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (bot_id, operation_id),
    FOREIGN KEY (bot_id, candidate_generation_id, component_id)
        REFERENCES generations(bot_id, generation_id, component_id),
    FOREIGN KEY (bot_id, old_generation_id, component_id)
        REFERENCES generations(bot_id, generation_id, component_id),
    CHECK (
        (operation_kind = 'DISABLE' AND candidate_generation_id IS NULL)
        OR (operation_kind IN ('INSTALL', 'RELOAD') AND candidate_generation_id IS NOT NULL)
    )
);

CREATE TABLE service_operations (
    bot_id text NOT NULL REFERENCES forest_kernel.bot_identity(bot_id),
    component_id text NOT NULL,
    operation_id text NOT NULL,
    service_name text NOT NULL,
    input_digest text NOT NULL CHECK (input_digest ~ '^sha256:[a-f0-9]{64}$'),
    causal_root_kind text NOT NULL CHECK (causal_root_kind <> '' AND length(causal_root_kind) <= 128),
    causal_root_id text NOT NULL CHECK (causal_root_id <> '' AND length(causal_root_id) <= 256),
    status text NOT NULL CHECK (status IN ('ACTIVE','SUCCEEDED','FAILED','UNKNOWN')),
    active_call_id text NOT NULL,
    active_generation_id text NOT NULL,
    deadline_at timestamptz NOT NULL,
    outcome_payload_id text,
    outcome_digest text,
    outcome_owner_id text,
    error_code text,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    completed_at timestamptz,
    PRIMARY KEY (bot_id, component_id, operation_id),
    FOREIGN KEY (bot_id, active_generation_id, component_id)
        REFERENCES generations(bot_id, generation_id, component_id),
    CHECK (
        (status='ACTIVE' AND outcome_payload_id IS NULL AND outcome_digest IS NULL
            AND outcome_owner_id IS NULL AND error_code IS NULL AND completed_at IS NULL)
        OR (status='SUCCEEDED' AND outcome_payload_id IS NOT NULL
            AND outcome_digest IS NOT NULL AND outcome_owner_id IS NOT NULL
            AND error_code IS NULL AND completed_at IS NOT NULL)
        OR (status='FAILED' AND outcome_payload_id IS NOT NULL
            AND outcome_digest IS NOT NULL AND outcome_owner_id IS NOT NULL
            AND error_code IS NOT NULL AND completed_at IS NOT NULL)
        OR (status='UNKNOWN' AND outcome_payload_id IS NULL AND outcome_digest IS NULL
            AND outcome_owner_id IS NULL AND error_code IS NOT NULL
            AND completed_at IS NOT NULL)
    ),
    CHECK ((outcome_payload_id IS NULL) = (outcome_digest IS NULL)),
    CHECK ((outcome_payload_id IS NULL) = (outcome_owner_id IS NULL))
);

CREATE TABLE provisioning_receipts (
    bot_id text PRIMARY KEY REFERENCES forest_kernel.bot_identity(bot_id),
    deployment_id text NOT NULL CHECK (octet_length(deployment_id) BETWEEN 1 AND 128),
    job_id text NOT NULL UNIQUE CHECK (octet_length(job_id) BETWEEN 1 AND 256),
    storage_profile_ref text NOT NULL CHECK (octet_length(storage_profile_ref) BETWEEN 1 AND 512),
    storage_generation bigint NOT NULL CHECK (storage_generation > 0),
    bootstrap_profile_ref text NOT NULL CHECK (octet_length(bootstrap_profile_ref) BETWEEN 1 AND 512),
    bootstrap_manifest_digest text NOT NULL CHECK (
        bootstrap_manifest_digest ~ '^sha256:[a-f0-9]{64}$'
    ),
    completed_at timestamptz NOT NULL DEFAULT clock_timestamp()
);

CREATE FUNCTION reject_provisioning_receipt_change()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    RAISE EXCEPTION 'Bot provisioning receipt is immutable';
END;
$$;

CREATE TRIGGER provisioning_receipt_immutable
    BEFORE UPDATE OR DELETE ON provisioning_receipts
    FOR EACH ROW EXECUTE FUNCTION reject_provisioning_receipt_change();

CREATE INDEX service_operations_status ON service_operations(
    bot_id, component_id, status, deadline_at
);

CREATE TABLE service_call_attempts (
    bot_id text NOT NULL,
    call_id text NOT NULL,
    component_id text NOT NULL,
    operation_id text NOT NULL,
    generation_id text NOT NULL,
    worker_incarnation text NOT NULL,
    service_name text NOT NULL,
    graph_id text NOT NULL,
    graph_revision bigint NOT NULL CHECK (graph_revision > 0),
    admission_lane text NOT NULL CHECK (admission_lane IN ('DATA','CONTROL')),
    owned_work boolean NOT NULL,
    parent_call_id text,
    causal_root_kind text NOT NULL CHECK (causal_root_kind <> '' AND length(causal_root_kind) <= 128),
    causal_root_id text NOT NULL CHECK (causal_root_id <> '' AND length(causal_root_id) <= 256),
    deadline_at timestamptz NOT NULL,
    status text NOT NULL CHECK (
        status IN ('ACTIVE','SUCCEEDED','FAILED','UNKNOWN','REPLAYED','REJECTED')
    ),
    error_code text,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    completed_at timestamptz,
    PRIMARY KEY (bot_id, call_id),
    FOREIGN KEY (bot_id, component_id, operation_id)
        REFERENCES service_operations(bot_id, component_id, operation_id),
    FOREIGN KEY (bot_id, generation_id, component_id)
        REFERENCES generations(bot_id, generation_id, component_id),
    CHECK (
        (status='ACTIVE' AND error_code IS NULL AND completed_at IS NULL)
        OR (status='SUCCEEDED' AND error_code IS NULL AND completed_at IS NOT NULL)
        OR (status='REPLAYED' AND completed_at IS NOT NULL)
        OR (status IN ('FAILED','UNKNOWN','REJECTED') AND error_code IS NOT NULL
            AND completed_at IS NOT NULL)
    )
);
CREATE INDEX service_call_attempts_operation ON service_call_attempts(
    bot_id, component_id, operation_id, created_at, call_id
);
CREATE INDEX service_call_attempts_generation ON service_call_attempts(
    bot_id, generation_id, status
);
CREATE INDEX service_call_attempts_causal_root ON service_call_attempts(
    bot_id, causal_root_kind, causal_root_id, component_id, created_at, call_id
);

CREATE TABLE state_heads (
    bot_id text NOT NULL REFERENCES forest_kernel.bot_identity(bot_id),
    component_id text NOT NULL,
    state_key text NOT NULL,
    revision bigint NOT NULL CHECK (revision >= 0),
    deleted boolean NOT NULL DEFAULT false,
    value_payload_id text,
    value_digest text,
    value_owner_id text,
    PRIMARY KEY (bot_id, component_id, state_key),
    CHECK ((value_payload_id IS NULL) = (value_digest IS NULL)),
    CHECK ((value_payload_id IS NULL) = (value_owner_id IS NULL)),
    CHECK (revision = 0 OR deleted OR value_payload_id IS NOT NULL)
);

CREATE TABLE state_steps (
    bot_id text NOT NULL,
    component_id text NOT NULL,
    operation_id text NOT NULL,
    generation_id text NOT NULL,
    call_id text NOT NULL,
    state_key text NOT NULL,
    expected_revision bigint NOT NULL CHECK (expected_revision >= 0),
    revision bigint NOT NULL CHECK (revision = expected_revision + 1),
    input_digest text NOT NULL CHECK (input_digest ~ '^sha256:[a-f0-9]{64}$'),
    value_payload_id text NOT NULL,
    value_digest text NOT NULL CHECK (value_digest ~ '^sha256:[a-f0-9]{64}$'),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (bot_id, component_id, operation_id),
    UNIQUE (bot_id, component_id, state_key, revision),
    FOREIGN KEY (bot_id, generation_id, component_id)
        REFERENCES generations(bot_id, generation_id, component_id)
);

CREATE TABLE runtime_graph_clock (
    bot_id text PRIMARY KEY REFERENCES forest_kernel.bot_identity(bot_id),
    next_revision bigint NOT NULL DEFAULT 1 CHECK (next_revision > 0)
);

CREATE TABLE runtime_graphs (
    bot_id text NOT NULL REFERENCES forest_kernel.bot_identity(bot_id),
    graph_id text NOT NULL,
    graph_revision bigint NOT NULL CHECK (graph_revision > 0),
    graph_digest text NOT NULL CHECK (graph_digest ~ '^sha256:[a-f0-9]{64}$'),
    activation_id text NOT NULL,
    source_graph_id text,
    state text NOT NULL CHECK (state IN ('PREPARED', 'ACTIVE', 'RETIRED', 'FAILED', 'STOPPED')),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    published_at timestamptz,
    PRIMARY KEY (bot_id, graph_id),
    UNIQUE (bot_id, graph_revision),
    UNIQUE (bot_id, activation_id),
    FOREIGN KEY (bot_id, source_graph_id) REFERENCES runtime_graphs(bot_id, graph_id)
);

CREATE TABLE runtime_graph_modules (
    bot_id text NOT NULL,
    graph_id text NOT NULL,
    component_id text NOT NULL,
    generation_id text NOT NULL,
    package_id text NOT NULL,
    package_version text NOT NULL,
    package_digest text NOT NULL CHECK (package_digest ~ '^sha256:[a-f0-9]{64}$'),
    driver text NOT NULL,
    trust_level text NOT NULL CHECK (trust_level IN ('official', 'reviewed', 'installed')),
    descriptor_digest text NOT NULL CHECK (descriptor_digest ~ '^sha256:[a-f0-9]{64}$'),
    route_contract_digest text NOT NULL CHECK (route_contract_digest ~ '^sha256:[a-f0-9]{64}$'),
    management_contract_digest text NOT NULL CHECK (
        management_contract_digest ~ '^sha256:[a-f0-9]{64}$'
    ),
    management_ui_digest text NOT NULL CHECK (management_ui_digest ~ '^sha256:[a-f0-9]{64}$'),
    data_admission boolean NOT NULL,
    control_admission boolean NOT NULL,
    storage_schema_id text,
    storage_schema_version text,
    storage_namespace text,
    storage_baseline_sha256 text,
    storage_package_digest text CHECK (
        storage_package_digest IS NULL OR storage_package_digest ~ '^sha256:[a-f0-9]{64}$'
    ),
    storage_repository_contract text,
    storage_execution_profile text CHECK (
        storage_execution_profile IS NULL
        OR storage_execution_profile IN ('declarative', 'official')
    ),
    PRIMARY KEY (bot_id, graph_id, component_id),
    FOREIGN KEY (bot_id, graph_id) REFERENCES runtime_graphs(bot_id, graph_id),
    FOREIGN KEY (bot_id, generation_id, component_id, package_digest)
        REFERENCES generations(bot_id, generation_id, component_id, package_digest),
    CHECK (
        (storage_schema_id IS NULL AND storage_schema_version IS NULL
            AND storage_namespace IS NULL AND storage_baseline_sha256 IS NULL
            AND storage_package_digest IS NULL AND storage_repository_contract IS NULL
            AND storage_execution_profile IS NULL)
        OR (storage_schema_id IS NOT NULL AND storage_schema_version IS NOT NULL
            AND storage_namespace IS NOT NULL AND storage_baseline_sha256 IS NOT NULL
            AND storage_package_digest IS NOT NULL AND storage_repository_contract IS NOT NULL
            AND storage_execution_profile IS NOT NULL)
    )
);

CREATE TABLE runtime_graph_dependencies (
    bot_id text NOT NULL,
    graph_id text NOT NULL,
    component_id text NOT NULL,
    required_component_id text NOT NULL,
    PRIMARY KEY (bot_id, graph_id, component_id, required_component_id),
    FOREIGN KEY (bot_id, graph_id, component_id)
        REFERENCES runtime_graph_modules(bot_id, graph_id, component_id),
    FOREIGN KEY (bot_id, graph_id, required_component_id)
        REFERENCES runtime_graph_modules(bot_id, graph_id, component_id),
    CHECK (component_id <> required_component_id)
);

CREATE TABLE runtime_graph_retention_dependencies (
    bot_id text NOT NULL,
    graph_id text NOT NULL,
    component_id text NOT NULL,
    required_component_id text NOT NULL,
    PRIMARY KEY (bot_id, graph_id, component_id, required_component_id),
    FOREIGN KEY (bot_id, graph_id, component_id)
        REFERENCES runtime_graph_modules(bot_id, graph_id, component_id),
    FOREIGN KEY (bot_id, graph_id, required_component_id)
        REFERENCES runtime_graph_modules(bot_id, graph_id, component_id),
    CHECK (component_id <> required_component_id)
);

CREATE TABLE runtime_graph_lifecycle_facts (
    bot_id text NOT NULL,
    graph_id text NOT NULL,
    component_id text NOT NULL,
    sequence bigint NOT NULL CHECK (sequence > 0),
    state text NOT NULL CHECK (state IN (
        'DISCOVERED', 'VALIDATED', 'PREPARED', 'ACTIVE', 'QUIESCING',
        'QUIESCENT', 'STOPPED', 'FAILED'
    )),
    error_code text,
    occurred_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (bot_id, graph_id, component_id, sequence),
    FOREIGN KEY (bot_id, graph_id, component_id)
        REFERENCES runtime_graph_modules(bot_id, graph_id, component_id)
);

CREATE TABLE runtime_graph_head (
    bot_id text PRIMARY KEY REFERENCES forest_kernel.bot_identity(bot_id),
    graph_id text,
    graph_revision bigint NOT NULL CHECK (graph_revision > 0),
    published_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    FOREIGN KEY (bot_id, graph_id) REFERENCES runtime_graphs(bot_id, graph_id),
    UNIQUE (bot_id, graph_revision)
);

CREATE TABLE graph_replacement_requests (
    bot_id text NOT NULL REFERENCES forest_kernel.bot_identity(bot_id),
    operation_id text NOT NULL CHECK (octet_length(operation_id) BETWEEN 1 AND 256),
    input_digest text NOT NULL CHECK (input_digest ~ '^sha256:[a-f0-9]{64}$'),
    requested_by text NOT NULL CHECK (octet_length(requested_by) BETWEEN 1 AND 256),
    expected_graph_revision bigint NOT NULL CHECK (expected_graph_revision > 0),
    selection_revision bigint CHECK (selection_revision IS NULL OR selection_revision >= 0),
    selection_digest text CHECK (
        selection_digest IS NULL OR selection_digest ~ '^sha256:[a-f0-9]{64}$'
    ),
    target_component_id text CHECK (
        target_component_id IS NULL
        OR target_component_id ~ '^[a-z0-9][a-z0-9._-]{0,127}$'
    ),
    activation_id text NOT NULL,
    state text NOT NULL DEFAULT 'QUEUED'
        CHECK (state IN ('QUEUED','RUNNING','SUCCEEDED','FAILED','CANCELLED')),
    owner_host_id text,
    owner_incarnation text,
    owner_epoch bigint,
    lease_until timestamptz NOT NULL DEFAULT '1970-01-01 UTC',
    attempt_count integer NOT NULL DEFAULT 0 CHECK (attempt_count >= 0),
    result_graph_id text,
    result_graph_revision bigint CHECK (result_graph_revision IS NULL OR result_graph_revision > 0),
    error_code text CHECK (error_code IS NULL OR octet_length(error_code) BETWEEN 1 AND 128),
    cancel_operation_id text CHECK (
        cancel_operation_id IS NULL OR octet_length(cancel_operation_id) BETWEEN 1 AND 256
    ),
    cancelled_by text CHECK (cancelled_by IS NULL OR octet_length(cancelled_by) BETWEEN 1 AND 256),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (bot_id,operation_id),
    UNIQUE (bot_id,activation_id),
    FOREIGN KEY (bot_id,result_graph_id) REFERENCES runtime_graphs(bot_id,graph_id),
    CHECK ((selection_revision IS NULL) = (selection_digest IS NULL)),
    CHECK (selection_revision IS NULL OR target_component_id IS NOT NULL),
    CHECK ((owner_host_id IS NULL) = (owner_incarnation IS NULL)),
    CHECK ((owner_host_id IS NULL) = (owner_epoch IS NULL)),
    CHECK ((cancel_operation_id IS NULL) = (cancelled_by IS NULL)),
    CHECK (
        (state='QUEUED' AND owner_host_id IS NULL AND attempt_count=0
            AND result_graph_id IS NULL AND result_graph_revision IS NULL AND error_code IS NULL
            AND cancel_operation_id IS NULL)
        OR (state='RUNNING' AND owner_host_id IS NOT NULL AND attempt_count>0
            AND result_graph_id IS NULL AND result_graph_revision IS NULL AND error_code IS NULL
            AND cancel_operation_id IS NULL)
        OR (state='SUCCEEDED' AND owner_host_id IS NOT NULL AND attempt_count>0
            AND result_graph_id IS NOT NULL AND result_graph_revision IS NOT NULL
            AND error_code IS NULL AND cancel_operation_id IS NULL)
        OR (state='FAILED' AND owner_host_id IS NOT NULL AND attempt_count>0
            AND result_graph_id IS NULL AND result_graph_revision IS NULL
            AND error_code IS NOT NULL AND cancel_operation_id IS NULL)
        OR (state='CANCELLED' AND owner_host_id IS NULL AND attempt_count=0
            AND result_graph_id IS NULL AND result_graph_revision IS NULL
            AND error_code IS NULL AND cancel_operation_id IS NOT NULL)
    )
);
CREATE INDEX graph_replacement_recent
    ON graph_replacement_requests(bot_id,created_at DESC,operation_id DESC);
CREATE UNIQUE INDEX graph_replacement_cancel_operation_unique
    ON graph_replacement_requests(bot_id,cancel_operation_id)
    WHERE cancel_operation_id IS NOT NULL;

CREATE FUNCTION reject_fact_mutation() RETURNS trigger
LANGUAGE plpgsql SET search_path FROM CURRENT AS $$
BEGIN
    RAISE EXCEPTION 'Forest immutable kernel fact cannot be changed' USING ERRCODE = '55000';
END;
$$;

CREATE FUNCTION generation_identity_guard() RETURNS trigger
LANGUAGE plpgsql SET search_path FROM CURRENT AS $$
BEGIN
    IF ROW(
        OLD.bot_id, OLD.generation_id, OLD.component_id, OLD.package_digest,
        OLD.config_revision, OLD.config_payload_id, OLD.config_payload_digest, OLD.instance_id,
        OLD.worker_incarnation, OLD.host_incarnation, OLD.host_owner_epoch,
        OLD.state_schema, OLD.state_contract_digest, OLD.capability_grants_digest,
        OLD.created_at
    ) IS DISTINCT FROM ROW(
        NEW.bot_id, NEW.generation_id, NEW.component_id, NEW.package_digest,
        NEW.config_revision, NEW.config_payload_id, NEW.config_payload_digest, NEW.instance_id,
        NEW.worker_incarnation, NEW.host_incarnation, NEW.host_owner_epoch,
        NEW.state_schema, NEW.state_contract_digest, NEW.capability_grants_digest,
        NEW.created_at
    ) THEN
        RAISE EXCEPTION 'Forest generation identity is immutable' USING ERRCODE = '55000';
    END IF;
    RETURN NEW;
END;
$$;

CREATE FUNCTION service_operation_guard() RETURNS trigger
LANGUAGE plpgsql SET search_path FROM CURRENT AS $$
BEGIN
    IF TG_OP = 'DELETE' THEN
        RAISE EXCEPTION 'Forest service operation cannot be deleted' USING ERRCODE = '55000';
    END IF;
    IF ROW(
        OLD.bot_id, OLD.component_id, OLD.operation_id, OLD.service_name,
        OLD.input_digest, OLD.causal_root_kind, OLD.causal_root_id,
        OLD.active_call_id, OLD.active_generation_id,
        OLD.deadline_at, OLD.created_at
    ) IS DISTINCT FROM ROW(
        NEW.bot_id, NEW.component_id, NEW.operation_id, NEW.service_name,
        NEW.input_digest, NEW.causal_root_kind, NEW.causal_root_id,
        NEW.active_call_id, NEW.active_generation_id,
        NEW.deadline_at, NEW.created_at
    ) THEN
        RAISE EXCEPTION 'Forest service operation identity is immutable' USING ERRCODE = '55000';
    END IF;
    IF OLD.status <> 'ACTIVE' OR NEW.status NOT IN ('SUCCEEDED','FAILED','UNKNOWN') THEN
        RAISE EXCEPTION 'Illegal Forest service operation transition' USING ERRCODE = '55000';
    END IF;
    RETURN NEW;
END;
$$;

CREATE FUNCTION service_call_attempt_guard() RETURNS trigger
LANGUAGE plpgsql SET search_path FROM CURRENT AS $$
BEGIN
    IF TG_OP = 'DELETE' THEN
        RAISE EXCEPTION 'Forest service call attempt cannot be deleted' USING ERRCODE = '55000';
    END IF;
    IF ROW(
        OLD.bot_id, OLD.call_id, OLD.component_id, OLD.operation_id,
        OLD.generation_id, OLD.worker_incarnation, OLD.service_name,
        OLD.graph_id, OLD.graph_revision, OLD.admission_lane, OLD.owned_work,
        OLD.parent_call_id, OLD.causal_root_kind, OLD.causal_root_id,
        OLD.deadline_at, OLD.created_at
    ) IS DISTINCT FROM ROW(
        NEW.bot_id, NEW.call_id, NEW.component_id, NEW.operation_id,
        NEW.generation_id, NEW.worker_incarnation, NEW.service_name,
        NEW.graph_id, NEW.graph_revision, NEW.admission_lane, NEW.owned_work,
        NEW.parent_call_id, NEW.causal_root_kind, NEW.causal_root_id,
        NEW.deadline_at, NEW.created_at
    ) THEN
        RAISE EXCEPTION 'Forest service call attempt identity is immutable' USING ERRCODE = '55000';
    END IF;
    IF OLD.status <> 'ACTIVE' OR NEW.status NOT IN ('SUCCEEDED','FAILED','UNKNOWN') THEN
        RAISE EXCEPTION 'Illegal Forest service call attempt transition' USING ERRCODE = '55000';
    END IF;
    RETURN NEW;
END;
$$;

CREATE FUNCTION runtime_graph_identity_guard() RETURNS trigger
LANGUAGE plpgsql SET search_path FROM CURRENT AS $$
BEGIN
    IF ROW(
        OLD.bot_id, OLD.graph_id, OLD.graph_revision, OLD.graph_digest,
        OLD.activation_id, OLD.created_at
    ) IS DISTINCT FROM ROW(
        NEW.bot_id, NEW.graph_id, NEW.graph_revision, NEW.graph_digest,
        NEW.activation_id, NEW.created_at
    ) THEN
        RAISE EXCEPTION 'Forest runtime graph identity is immutable' USING ERRCODE = '55000';
    END IF;
    RETURN NEW;
END;
$$;

CREATE FUNCTION graph_replacement_guard() RETURNS trigger
LANGUAGE plpgsql SET search_path FROM CURRENT AS $$
BEGIN
    IF TG_OP = 'DELETE' THEN
        RAISE EXCEPTION 'Forest graph replacement cannot be deleted' USING ERRCODE = '55000';
    END IF;
    IF ROW(
        OLD.bot_id, OLD.operation_id, OLD.input_digest, OLD.requested_by,
        OLD.expected_graph_revision, OLD.selection_revision, OLD.selection_digest,
        OLD.target_component_id, OLD.activation_id, OLD.created_at
    ) IS DISTINCT FROM ROW(
        NEW.bot_id, NEW.operation_id, NEW.input_digest, NEW.requested_by,
        NEW.expected_graph_revision, NEW.selection_revision, NEW.selection_digest,
        NEW.target_component_id, NEW.activation_id, NEW.created_at
    ) THEN
        RAISE EXCEPTION 'Forest graph replacement identity is immutable' USING ERRCODE = '55000';
    END IF;
    IF OLD.state IN ('SUCCEEDED','FAILED','CANCELLED') THEN
        RAISE EXCEPTION 'Forest graph replacement terminal state is immutable'
            USING ERRCODE = '55000';
    END IF;
    IF NOT (
        (OLD.state='QUEUED' AND NEW.state IN ('RUNNING','CANCELLED'))
        OR (OLD.state='RUNNING' AND NEW.state IN ('RUNNING','SUCCEEDED','FAILED'))
    ) THEN
        RAISE EXCEPTION 'Illegal graph replacement transition' USING ERRCODE = '55000';
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER host_incarnations_immutable BEFORE UPDATE OR DELETE ON host_incarnations
    FOR EACH ROW EXECUTE FUNCTION reject_fact_mutation();
CREATE TRIGGER generation_identity BEFORE UPDATE ON generations
    FOR EACH ROW EXECUTE FUNCTION generation_identity_guard();
CREATE TRIGGER service_operation_transition BEFORE UPDATE OR DELETE ON service_operations
    FOR EACH ROW EXECUTE FUNCTION service_operation_guard();
CREATE TRIGGER service_call_attempt_transition
    BEFORE UPDATE OR DELETE ON service_call_attempts
    FOR EACH ROW EXECUTE FUNCTION service_call_attempt_guard();
CREATE TRIGGER runtime_graph_identity BEFORE UPDATE ON runtime_graphs
    FOR EACH ROW EXECUTE FUNCTION runtime_graph_identity_guard();
CREATE TRIGGER graph_replacement_transition BEFORE UPDATE OR DELETE ON graph_replacement_requests
    FOR EACH ROW EXECUTE FUNCTION graph_replacement_guard();
CREATE TRIGGER state_steps_immutable BEFORE UPDATE OR DELETE ON state_steps
    FOR EACH ROW EXECUTE FUNCTION reject_fact_mutation();
CREATE TRIGGER rescue_policy_operations_immutable
    BEFORE UPDATE OR DELETE ON rescue_policy_operations
    FOR EACH ROW EXECUTE FUNCTION reject_fact_mutation();
CREATE TRIGGER runtime_graph_modules_immutable BEFORE UPDATE OR DELETE ON runtime_graph_modules
    FOR EACH ROW EXECUTE FUNCTION reject_fact_mutation();
CREATE TRIGGER runtime_graph_dependencies_immutable BEFORE UPDATE OR DELETE ON runtime_graph_dependencies
    FOR EACH ROW EXECUTE FUNCTION reject_fact_mutation();
CREATE TRIGGER runtime_graph_lifecycle_facts_immutable
    BEFORE UPDATE OR DELETE ON runtime_graph_lifecycle_facts
    FOR EACH ROW EXECUTE FUNCTION reject_fact_mutation();

INSERT INTO runtime_state(bot_id) SELECT bot_id FROM forest_kernel.bot_identity;
INSERT INTO rescue_policy_state(bot_id) SELECT bot_id FROM forest_kernel.bot_identity;
INSERT INTO host_lease(bot_id) SELECT bot_id FROM forest_kernel.bot_identity;
INSERT INTO runtime_graph_clock(bot_id) SELECT bot_id FROM forest_kernel.bot_identity;
