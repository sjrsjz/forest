CREATE SCHEMA forest_deployment;

CREATE TABLE forest_deployment.deployment_identity (
    singleton boolean PRIMARY KEY DEFAULT true CHECK (singleton),
    deployment_id text NOT NULL UNIQUE CHECK (octet_length(deployment_id) BETWEEN 1 AND 128),
    baseline_sha256 text NOT NULL CHECK (baseline_sha256 ~ '^[a-f0-9]{64}$'),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp()
);

CREATE TABLE forest_deployment.schema_bundles (
    deployment_id text NOT NULL REFERENCES forest_deployment.deployment_identity(deployment_id),
    schema_id text NOT NULL,
    component_id text NOT NULL,
    schema_version text NOT NULL,
    scope text NOT NULL CHECK (scope = 'deployment'),
    namespace name NOT NULL,
    baseline_sha256 text NOT NULL CHECK (baseline_sha256 ~ '^[0-9a-f]{64}$'),
    package_digest text NOT NULL CHECK (package_digest ~ '^sha256:[0-9a-f]{64}$'),
    repository_contract text NOT NULL,
    execution_profile text NOT NULL CHECK (execution_profile IN ('declarative', 'official')),
    state text NOT NULL CHECK (state IN ('READY', 'DETACHED')),
    installed_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    detached_at timestamptz,
    PRIMARY KEY (deployment_id, schema_id),
    UNIQUE (namespace),
    CHECK (
        (state = 'READY' AND detached_at IS NULL)
        OR (state = 'DETACHED' AND detached_at IS NOT NULL)
    )
);

CREATE TABLE forest_deployment.runtime_graph_clock (
    deployment_id text PRIMARY KEY REFERENCES forest_deployment.deployment_identity(deployment_id),
    next_revision bigint NOT NULL DEFAULT 0 CHECK (next_revision >= 0),
    active_revision bigint NOT NULL DEFAULT 0 CHECK (active_revision >= 0),
    active_graph_id text,
    CHECK (active_revision <= next_revision)
);

CREATE TABLE forest_deployment.runtime_graphs (
    deployment_id text NOT NULL REFERENCES forest_deployment.deployment_identity(deployment_id),
    graph_id text NOT NULL,
    graph_revision bigint NOT NULL CHECK (graph_revision > 0),
    graph_digest text NOT NULL CHECK (graph_digest ~ '^sha256:[0-9a-f]{64}$'),
    activation_id text NOT NULL,
    source_graph_id text,
    source_revision bigint NOT NULL CHECK (source_revision >= 0),
    owner_epoch bigint NOT NULL CHECK (owner_epoch > 0),
    state text NOT NULL CHECK (state IN ('PREPARED','ACTIVE','RETIRED','FAILED')),
    prepared_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    published_at timestamptz,
    retired_at timestamptz,
    PRIMARY KEY (deployment_id,graph_id),
    UNIQUE (deployment_id,graph_revision),
    UNIQUE (deployment_id,activation_id),
    FOREIGN KEY (deployment_id,source_graph_id)
        REFERENCES forest_deployment.runtime_graphs(deployment_id,graph_id)
);

CREATE TABLE forest_deployment.runtime_generations (
    deployment_id text NOT NULL REFERENCES forest_deployment.deployment_identity(deployment_id),
    generation_id text NOT NULL,
    component_id text NOT NULL,
    binding_digest text NOT NULL CHECK (binding_digest ~ '^sha256:[0-9a-f]{64}$'),
    binding jsonb NOT NULL,
    owner_epoch bigint NOT NULL CHECK (owner_epoch > 0),
    state text NOT NULL CHECK (state IN ('PREPARED','ACTIVE','QUIESCING','STOPPED','FAILED')),
    failure_code text CHECK (failure_code IS NULL OR failure_code ~ '^[A-Z][A-Z0-9_]{0,127}$'),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (deployment_id,generation_id)
);

CREATE TABLE forest_deployment.runtime_graph_members (
    deployment_id text NOT NULL,
    graph_id text NOT NULL,
    component_id text NOT NULL,
    generation_id text NOT NULL,
    data_admission boolean NOT NULL,
    control_admission boolean NOT NULL,
    route_epoch bigint NOT NULL DEFAULT 1 CHECK (route_epoch > 0),
    PRIMARY KEY (deployment_id,graph_id,component_id),
    UNIQUE (deployment_id,graph_id,generation_id),
    FOREIGN KEY (deployment_id,graph_id)
        REFERENCES forest_deployment.runtime_graphs(deployment_id,graph_id),
    FOREIGN KEY (deployment_id,generation_id)
        REFERENCES forest_deployment.runtime_generations(deployment_id,generation_id)
);

ALTER TABLE forest_deployment.runtime_graph_clock
    ADD FOREIGN KEY (deployment_id,active_graph_id)
    REFERENCES forest_deployment.runtime_graphs(deployment_id,graph_id);

CREATE TABLE forest_deployment.runtime_package_selection (
    deployment_id text NOT NULL REFERENCES forest_deployment.deployment_identity(deployment_id),
    component_id text NOT NULL CHECK (component_id ~ '^[a-z0-9][a-z0-9._-]{0,127}$'),
    package_digest text NOT NULL CHECK (package_digest ~ '^sha256:[a-f0-9]{64}$'),
    configuration jsonb NOT NULL,
    graph_revision bigint NOT NULL CHECK (graph_revision > 0),
    PRIMARY KEY (deployment_id,component_id)
);

CREATE TABLE forest_deployment.package_service_operations (
    deployment_id text NOT NULL REFERENCES forest_deployment.deployment_identity(deployment_id),
    component_id text NOT NULL,
    operation_id text NOT NULL CHECK (octet_length(operation_id) BETWEEN 1 AND 256),
    service_id text NOT NULL,
    input_digest text NOT NULL CHECK (input_digest ~ '^sha256:[a-f0-9]{64}$'),
    causal_root_kind text NOT NULL,
    causal_root_id text NOT NULL,
    generation_id text NOT NULL,
    call_id text NOT NULL,
    deadline_at timestamptz NOT NULL,
    status text NOT NULL CHECK (status IN ('ACTIVE','SUCCEEDED','FAILED','UNKNOWN')),
    outcome jsonb,
    error_code text CHECK (error_code ~ '^[A-Z][A-Z0-9_]{0,127}$'),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    completed_at timestamptz,
    PRIMARY KEY (deployment_id,component_id,operation_id),
    UNIQUE (deployment_id,call_id),
    FOREIGN KEY (deployment_id,generation_id)
        REFERENCES forest_deployment.runtime_generations(deployment_id,generation_id),
    CHECK ((status='ACTIVE' AND outcome IS NULL AND completed_at IS NULL)
        OR (status='UNKNOWN' AND outcome IS NULL AND completed_at IS NOT NULL)
        OR (status IN ('SUCCEEDED','FAILED') AND outcome IS NOT NULL AND completed_at IS NOT NULL))
);
CREATE INDEX package_service_operations_status
    ON forest_deployment.package_service_operations(deployment_id,status,deadline_at);

CREATE TABLE forest_deployment.runtime_graph_change_requests (
    deployment_id text NOT NULL REFERENCES forest_deployment.deployment_identity(deployment_id),
    operation_id text NOT NULL CHECK (octet_length(operation_id) BETWEEN 1 AND 256),
    operation_kind text NOT NULL CHECK (operation_kind IN ('ADD','REPLACE','CONFIGURE')),
    input_digest text NOT NULL CHECK (input_digest ~ '^sha256:[0-9a-f]{64}$'),
    plan_digest text NOT NULL CHECK (plan_digest ~ '^sha256:[0-9a-f]{64}$'),
    requested_by text NOT NULL CHECK (octet_length(requested_by) BETWEEN 1 AND 128),
    target_component_id text NOT NULL CHECK (target_component_id ~ '^[a-z0-9][a-z0-9._-]{0,127}$'),
    package_digest text NOT NULL CHECK (package_digest ~ '^sha256:[0-9a-f]{64}$'),
    configuration_revision bigint CHECK (configuration_revision IS NULL OR configuration_revision > 0),
    configuration_payload_digest text CHECK (
        configuration_payload_digest IS NULL OR configuration_payload_digest ~ '^sha256:[0-9a-f]{64}$'
    ),
    expected_graph_revision bigint NOT NULL CHECK (expected_graph_revision > 0),
    activation_id text NOT NULL,
    receipt_ticket_id text NOT NULL,
    state text NOT NULL DEFAULT 'QUEUED'
        CHECK (state IN ('QUEUED','RUNNING','SUCCEEDED','FAILED')),
    owner_epoch bigint CHECK (owner_epoch IS NULL OR owner_epoch > 0),
    attempt_count integer NOT NULL DEFAULT 0 CHECK (attempt_count >= 0),
    result_graph_id text,
    result_graph_revision bigint CHECK (result_graph_revision IS NULL OR result_graph_revision > 0),
    error_code text CHECK (error_code IS NULL OR error_code ~ '^[A-Z][A-Z0-9_]{0,127}$'),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (deployment_id,operation_id),
    UNIQUE (deployment_id,activation_id),
    FOREIGN KEY (deployment_id,result_graph_id)
        REFERENCES forest_deployment.runtime_graphs(deployment_id,graph_id),
    CHECK (
        (operation_kind='CONFIGURE' AND configuration_revision IS NOT NULL
            AND configuration_payload_digest IS NOT NULL)
        OR (operation_kind<>'CONFIGURE' AND configuration_revision IS NULL
            AND configuration_payload_digest IS NULL)
    ),
    CHECK (
        (state='QUEUED' AND owner_epoch IS NULL AND attempt_count=0
            AND result_graph_id IS NULL AND result_graph_revision IS NULL AND error_code IS NULL)
        OR (state='RUNNING' AND owner_epoch IS NOT NULL AND attempt_count>0
            AND result_graph_id IS NULL AND result_graph_revision IS NULL AND error_code IS NULL)
        OR (state='SUCCEEDED' AND owner_epoch IS NOT NULL AND attempt_count>0
            AND result_graph_id IS NOT NULL AND result_graph_revision IS NOT NULL AND error_code IS NULL)
        OR (state='FAILED' AND owner_epoch IS NOT NULL AND attempt_count>0
            AND result_graph_id IS NULL AND result_graph_revision IS NULL AND error_code IS NOT NULL)
    )
);
CREATE INDEX runtime_graph_change_queue
    ON forest_deployment.runtime_graph_change_requests(deployment_id,state,created_at,operation_id);

CREATE TABLE forest_deployment.supervisor_lease (
    singleton boolean PRIMARY KEY DEFAULT true CHECK (singleton),
    host_id text,
    incarnation text,
    owner_epoch bigint NOT NULL DEFAULT 0 CHECK (owner_epoch >= 0),
    expires_at timestamptz NOT NULL DEFAULT '1970-01-01 00:00:00+00'
);

CREATE TABLE forest_deployment.web_accounts (
    subject_id text PRIMARY KEY CHECK (octet_length(subject_id) BETWEEN 1 AND 128),
    username text NOT NULL UNIQUE CHECK (username ~ '^[a-z][a-z0-9._-]{2,63}$'),
    display_name text NOT NULL CHECK (octet_length(display_name) BETWEEN 1 AND 128),
    password_hash text NOT NULL CHECK (octet_length(password_hash) BETWEEN 1 AND 256),
    role text NOT NULL CHECK (role IN ('ADMIN','AUDITOR')),
    enabled boolean NOT NULL DEFAULT true,
    revision bigint NOT NULL DEFAULT 1 CHECK (revision > 0),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp()
);

CREATE TABLE forest_deployment.web_setup_state (
    deployment_id text PRIMARY KEY REFERENCES forest_deployment.deployment_identity(deployment_id),
    setup_id text NOT NULL UNIQUE CHECK (
        octet_length(setup_id) BETWEEN 22 AND 134
        AND setup_id ~ '^setup/[A-Za-z0-9_-]{16,128}$'
    ),
    state text NOT NULL CHECK (state IN ('PENDING','COMPLETED')),
    pairing_code_digest text CHECK (
        pairing_code_digest IS NULL OR pairing_code_digest ~ '^sha256:[a-f0-9]{64}$'
    ),
    issued_at timestamptz NOT NULL,
    expires_at timestamptz NOT NULL,
    consumed_at timestamptz,
    initial_admin_ref text REFERENCES forest_deployment.web_accounts(subject_id),
    revision bigint NOT NULL DEFAULT 1 CHECK (revision > 0),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    CHECK (expires_at > issued_at AND expires_at <= issued_at + interval '24 hours'),
    CHECK (
        (state='PENDING' AND pairing_code_digest IS NOT NULL
            AND consumed_at IS NULL AND initial_admin_ref IS NULL)
        OR
        (state='COMPLETED' AND pairing_code_digest IS NULL
            AND consumed_at IS NOT NULL AND initial_admin_ref IS NOT NULL)
    )
);

CREATE TABLE forest_deployment.web_account_operations (
    operation_id text PRIMARY KEY CHECK (octet_length(operation_id) BETWEEN 1 AND 256),
    operation_kind text NOT NULL CHECK (
        operation_kind IN ('SETUP','CREATE','UPDATE','PASSWORD','REVOKE_SESSIONS')
    ),
    input_digest text NOT NULL CHECK (input_digest ~ '^sha256:[a-f0-9]{64}$'),
    credential_key_id text CHECK (octet_length(credential_key_id) BETWEEN 1 AND 256),
    subject_id text NOT NULL REFERENCES forest_deployment.web_accounts(subject_id),
    resulting_revision bigint NOT NULL CHECK (resulting_revision > 0),
    result jsonb NOT NULL CHECK (jsonb_typeof(result)='object'),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    CHECK (
        (operation_kind IN ('SETUP','CREATE','PASSWORD')) =
        (credential_key_id IS NOT NULL)
    )
);

CREATE TABLE forest_deployment.web_sessions (
    session_id_hash text PRIMARY KEY CHECK (session_id_hash ~ '^[a-f0-9]{64}$'),
    csrf_hash text NOT NULL CHECK (csrf_hash ~ '^[a-f0-9]{64}$'),
    subject_id text NOT NULL REFERENCES forest_deployment.web_accounts(subject_id),
    authn_strength text NOT NULL CHECK (authn_strength='PASSWORD'),
    revision bigint NOT NULL DEFAULT 1 CHECK (revision > 0),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    expires_at timestamptz NOT NULL,
    revoked_at timestamptz,
    CHECK (expires_at > created_at)
);

CREATE INDEX web_sessions_subject_active_idx
    ON forest_deployment.web_sessions(subject_id,expires_at)
    WHERE revoked_at IS NULL;

CREATE TABLE forest_deployment.web_login_throttles (
    bucket_key text PRIMARY KEY CHECK (
        bucket_key ~ '^(identity|peer):[a-f0-9]{64}$'
    ),
    bucket_kind text NOT NULL CHECK (bucket_kind IN ('IDENTITY','PEER')),
    key_id text NOT NULL CHECK (octet_length(key_id) BETWEEN 1 AND 256),
    failure_count integer NOT NULL DEFAULT 0 CHECK (failure_count BETWEEN 0 AND 100000),
    window_started_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    blocked_until timestamptz,
    revision bigint NOT NULL DEFAULT 1 CHECK (revision > 0),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    CHECK (blocked_until IS NULL OR blocked_until >= window_started_at)
);

CREATE TABLE forest_deployment.web_authority_receipts (
    deployment_id text NOT NULL REFERENCES forest_deployment.deployment_identity(deployment_id),
    ticket_id text NOT NULL CHECK (octet_length(ticket_id) BETWEEN 1 AND 256),
    ticket_digest text NOT NULL CHECK (ticket_digest ~ '^sha256:[a-f0-9]{64}$'),
    issuer_id text NOT NULL CHECK (octet_length(issuer_id) BETWEEN 1 AND 128),
    key_id text NOT NULL CHECK (octet_length(key_id) BETWEEN 1 AND 256),
    web_subject_ref text NOT NULL CHECK (octet_length(web_subject_ref) BETWEEN 1 AND 128),
    web_role text NOT NULL CHECK (web_role IN ('ADMIN','AUDITOR')),
    session_ref text NOT NULL CHECK (octet_length(session_ref) BETWEEN 1 AND 128),
    session_revision bigint NOT NULL CHECK (session_revision > 0),
    target_scope_kind text NOT NULL CHECK (target_scope_kind='deployment'),
    target_scope_id text NOT NULL CHECK (target_scope_id='deployment'),
    target_component_id text NOT NULL CHECK (octet_length(target_component_id) BETWEEN 1 AND 128),
    target_generation_id text NOT NULL CHECK (octet_length(target_generation_id) BETWEEN 1 AND 256),
    target_graph_revision bigint NOT NULL CHECK (target_graph_revision > 0),
    target_operation_ref text NOT NULL CHECK (octet_length(target_operation_ref) BETWEEN 1 AND 256),
    action_key text NOT NULL CHECK (octet_length(action_key) BETWEEN 1 AND 200),
    input_digest text NOT NULL CHECK (input_digest ~ '^sha256:[a-f0-9]{64}$'),
    plan_digest text CHECK (plan_digest IS NULL OR plan_digest ~ '^sha256:[a-f0-9]{64}$'),
    issued_at timestamptz NOT NULL,
    expires_at timestamptz NOT NULL,
    accepted_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (deployment_id,ticket_id),
    UNIQUE (deployment_id,target_operation_ref),
    CHECK (expires_at > issued_at AND expires_at <= issued_at + interval '30 seconds')
);

ALTER TABLE forest_deployment.runtime_graph_change_requests
    ADD FOREIGN KEY (deployment_id,receipt_ticket_id)
    REFERENCES forest_deployment.web_authority_receipts(deployment_id,ticket_id);

CREATE TABLE forest_deployment.audit_records (
    deployment_id text NOT NULL REFERENCES forest_deployment.deployment_identity(deployment_id),
    audit_id text NOT NULL CHECK (
        octet_length(audit_id) BETWEEN 1 AND 128
        AND audit_id ~ '^[A-Za-z0-9][A-Za-z0-9_.:-]*$'
    ),
    action text NOT NULL CHECK (
        octet_length(action) BETWEEN 1 AND 200
        AND action ~ '^[A-Za-z0-9][A-Za-z0-9_.:-]*$'
    ),
    severity text NOT NULL CHECK (severity IN ('INFO','WARNING','ERROR','SECURITY')),
    producer_component_id text NOT NULL CHECK (
        octet_length(producer_component_id) BETWEEN 1 AND 128
    ),
    producer_generation_id text NOT NULL CHECK (
        octet_length(producer_generation_id) BETWEEN 1 AND 256
    ),
    origin_kind text NOT NULL CHECK (origin_kind='web.management'),
    actor_kind text NOT NULL CHECK (actor_kind='web.subject'),
    actor_id text NOT NULL CHECK (octet_length(actor_id) BETWEEN 1 AND 128),
    resource_kind text NOT NULL CHECK (resource_kind='deployment.component'),
    resource_id text NOT NULL CHECK (octet_length(resource_id) BETWEEN 1 AND 128),
    operation_kind text NOT NULL CHECK (operation_kind='web.action'),
    operation_id text NOT NULL CHECK (octet_length(operation_id) BETWEEN 1 AND 256),
    receipt_ticket_id text NOT NULL,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (deployment_id,audit_id),
    UNIQUE (deployment_id,operation_id),
    FOREIGN KEY (deployment_id,receipt_ticket_id)
        REFERENCES forest_deployment.web_authority_receipts(deployment_id,ticket_id)
);

CREATE INDEX deployment_audit_time_idx
    ON forest_deployment.audit_records(deployment_id,created_at DESC,audit_id DESC);
CREATE INDEX deployment_audit_action_idx
    ON forest_deployment.audit_records(deployment_id,action,created_at DESC,audit_id DESC);
CREATE INDEX deployment_audit_producer_idx
    ON forest_deployment.audit_records(
        deployment_id,producer_component_id,created_at DESC,audit_id DESC
    );
CREATE INDEX deployment_audit_actor_idx
    ON forest_deployment.audit_records(deployment_id,actor_kind,actor_id,created_at DESC);

CREATE INDEX web_login_throttles_expiry_idx
    ON forest_deployment.web_login_throttles(updated_at);

CREATE TABLE forest_deployment.storage_requests (
    request_id text PRIMARY KEY CHECK (request_id ~ '^storage-request/[a-f0-9]{64}$'),
    deployment_id text NOT NULL REFERENCES forest_deployment.deployment_identity(deployment_id),
    host_id text NOT NULL CHECK (host_id ~ '^host/[a-f0-9]{32}$'),
    bot_id text NOT NULL CHECK (octet_length(bot_id) BETWEEN 1 AND 128),
    storage_generation bigint NOT NULL CHECK (storage_generation > 0),
    profile_ref text NOT NULL CHECK (octet_length(profile_ref) BETWEEN 1 AND 512),
    dsn_secret_ref text NOT NULL CHECK (octet_length(dsn_secret_ref) BETWEEN 10 AND 512),
    credential_ref text NOT NULL CHECK (octet_length(credential_ref) BETWEEN 10 AND 512),
    credential_version bigint NOT NULL CHECK (credential_version > 0),
    credential_digest text NOT NULL CHECK (credential_digest ~ '^sha256:[a-f0-9]{64}$'),
    input_digest text NOT NULL CHECK (input_digest ~ '^sha256:[a-f0-9]{64}$'),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (deployment_id,bot_id,storage_generation),
    UNIQUE (deployment_id,profile_ref),
    UNIQUE (deployment_id,dsn_secret_ref),
    UNIQUE (deployment_id,credential_ref)
);

CREATE INDEX deployment_storage_requests_created_idx
    ON forest_deployment.storage_requests(deployment_id,created_at,request_id);

CREATE TABLE forest_deployment.storage_request_dispatch (
    request_id text PRIMARY KEY REFERENCES forest_deployment.storage_requests(request_id),
    state text NOT NULL DEFAULT 'PENDING'
        CHECK (state IN ('PENDING','SETTLED','BLOCKED')),
    attempt_count bigint NOT NULL DEFAULT 0 CHECK (attempt_count >= 0),
    next_attempt_at timestamptz DEFAULT clock_timestamp(),
    claim_token text,
    claim_owner_epoch bigint CHECK (claim_owner_epoch IS NULL OR claim_owner_epoch > 0),
    claim_expires_at timestamptz,
    last_error_code text CHECK (
        last_error_code IS NULL OR last_error_code ~ '^[A-Z][A-Z0-9_]{0,127}$'
    ),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    CHECK (
        (claim_token IS NULL AND claim_owner_epoch IS NULL AND claim_expires_at IS NULL)
        OR (claim_token IS NOT NULL AND claim_owner_epoch IS NOT NULL
            AND claim_expires_at IS NOT NULL)
    ),
    CHECK ((state='PENDING' AND next_attempt_at IS NOT NULL)
        OR (state<>'PENDING' AND next_attempt_at IS NULL))
);
CREATE INDEX deployment_storage_request_dispatch_due_idx
    ON forest_deployment.storage_request_dispatch(state,next_attempt_at,request_id);

CREATE TABLE forest_deployment.storage_request_resources (
    request_id text PRIMARY KEY REFERENCES forest_deployment.storage_requests(request_id),
    resource_id text NOT NULL UNIQUE CHECK (resource_id ~ '^resource/[a-f0-9-]{36}$'),
    bound_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (request_id,resource_id)
);

CREATE TABLE forest_deployment.storage_request_pins (
    request_id text PRIMARY KEY,
    resource_id text NOT NULL UNIQUE CHECK (resource_id ~ '^resource/[a-f0-9-]{36}$'),
    deployment_id text NOT NULL REFERENCES forest_deployment.deployment_identity(deployment_id),
    host_id text NOT NULL CHECK (host_id ~ '^host/[a-f0-9]{32}$'),
    profile_ref text NOT NULL CHECK (octet_length(profile_ref) BETWEEN 1 AND 512),
    resource_revision bigint NOT NULL CHECK (resource_revision >= 2),
    credential_digest text NOT NULL CHECK (credential_digest ~ '^sha256:[a-f0-9]{64}$'),
    physical_name text NOT NULL CHECK (physical_name ~ '^forest_bot_[a-f0-9]{24}$'),
    role_name text NOT NULL CHECK (role_name ~ '^forest_role_[a-f0-9]{24}$'),
    pinned_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    FOREIGN KEY (request_id,resource_id)
        REFERENCES forest_deployment.storage_request_resources(request_id,resource_id),
    UNIQUE (deployment_id,profile_ref)
);

CREATE TABLE forest_deployment.storage_profiles (
    profile_ref text PRIMARY KEY CHECK (octet_length(profile_ref) BETWEEN 1 AND 512),
    bot_id text NOT NULL CHECK (octet_length(bot_id) BETWEEN 1 AND 128),
    storage_generation bigint NOT NULL CHECK (storage_generation > 0),
    dsn_secret_ref text NOT NULL CHECK (octet_length(dsn_secret_ref) BETWEEN 10 AND 512),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (profile_ref,bot_id,storage_generation),
    UNIQUE (bot_id,storage_generation),
    UNIQUE (dsn_secret_ref)
);

CREATE TABLE forest_deployment.storage_profile_operations (
    operation_id text PRIMARY KEY CHECK (octet_length(operation_id) BETWEEN 1 AND 256),
    input_digest text NOT NULL CHECK (input_digest ~ '^sha256:[a-f0-9]{64}$'),
    profile_ref text NOT NULL REFERENCES forest_deployment.storage_profiles(profile_ref),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp()
);

CREATE TABLE forest_deployment.bootstrap_profiles (
    profile_ref text PRIMARY KEY CHECK (octet_length(profile_ref) BETWEEN 1 AND 512),
    bot_id text NOT NULL CHECK (octet_length(bot_id) BETWEEN 1 AND 128),
    storage_profile_ref text NOT NULL,
    storage_generation bigint NOT NULL CHECK (storage_generation > 0),
    manifest_digest text NOT NULL CHECK (manifest_digest ~ '^sha256:[a-f0-9]{64}$'),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    FOREIGN KEY (storage_profile_ref,bot_id,storage_generation)
        REFERENCES forest_deployment.storage_profiles(profile_ref,bot_id,storage_generation),
    UNIQUE (profile_ref,bot_id,storage_profile_ref,storage_generation),
    UNIQUE (profile_ref,bot_id,storage_profile_ref,storage_generation,manifest_digest)
);

CREATE TABLE forest_deployment.bootstrap_profile_components (
    profile_ref text NOT NULL REFERENCES forest_deployment.bootstrap_profiles(profile_ref),
    component_id text NOT NULL CHECK (component_id ~ '^[a-z0-9][a-z0-9._-]{0,127}$'),
    schema_digest text NOT NULL CHECK (schema_digest ~ '^sha256:[a-f0-9]{64}$'),
    package_digest text NOT NULL CHECK (package_digest ~ '^sha256:[a-f0-9]{64}$'),
    config_digest text NOT NULL CHECK (config_digest ~ '^sha256:[a-f0-9]{64}$'),
    config jsonb NOT NULL CHECK (jsonb_typeof(config)='object'),
    PRIMARY KEY (profile_ref,component_id)
);

CREATE TABLE forest_deployment.bootstrap_profile_operations (
    operation_id text PRIMARY KEY CHECK (octet_length(operation_id) BETWEEN 1 AND 256),
    input_digest text NOT NULL CHECK (input_digest ~ '^sha256:[a-f0-9]{64}$'),
    profile_ref text NOT NULL REFERENCES forest_deployment.bootstrap_profiles(profile_ref),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp()
);

CREATE TABLE forest_deployment.provisioning_jobs (
    job_id text PRIMARY KEY CHECK (octet_length(job_id) BETWEEN 1 AND 256),
    bot_id text NOT NULL UNIQUE CHECK (octet_length(bot_id) BETWEEN 1 AND 128),
    display_name text NOT NULL CHECK (octet_length(display_name) BETWEEN 1 AND 256),
    storage_profile_ref text NOT NULL,
    storage_generation bigint NOT NULL CHECK (storage_generation > 0),
    bootstrap_profile_ref text NOT NULL,
    bootstrap_manifest_digest text NOT NULL CHECK (
        bootstrap_manifest_digest ~ '^sha256:[a-f0-9]{64}$'
    ),
    state text NOT NULL CHECK (
        state IN ('QUEUED','RUNNING','RETRY_WAIT','BLOCKED_UNKNOWN','SUCCEEDED','FAILED','CANCELLED')
    ),
    attempt_no integer NOT NULL DEFAULT 0 CHECK (attempt_no >= 0),
    max_attempts integer NOT NULL CHECK (max_attempts BETWEEN 1 AND 20),
    revision bigint NOT NULL DEFAULT 1 CHECK (revision > 0),
    lease_owner text,
    lease_epoch bigint NOT NULL DEFAULT 0 CHECK (lease_epoch >= 0),
    lease_expires_at timestamptz,
    retry_not_before timestamptz,
    last_error_code text CHECK (
        last_error_code IS NULL OR last_error_code ~ '^[A-Z][A-Z0-9_]{0,127}$'
    ),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    completed_at timestamptz,
    FOREIGN KEY (storage_profile_ref,bot_id,storage_generation)
        REFERENCES forest_deployment.storage_profiles(profile_ref,bot_id,storage_generation),
    FOREIGN KEY (bootstrap_profile_ref,bot_id,storage_profile_ref,storage_generation)
        REFERENCES forest_deployment.bootstrap_profiles(
            profile_ref,bot_id,storage_profile_ref,storage_generation
        ),
    CHECK ((state='RUNNING') = (lease_owner IS NOT NULL)),
    CHECK ((state='RUNNING') = (lease_expires_at IS NOT NULL)),
    CHECK ((state='RETRY_WAIT') = (retry_not_before IS NOT NULL)),
    CHECK ((state IN ('SUCCEEDED','FAILED','CANCELLED')) = (completed_at IS NOT NULL)),
    CHECK (state NOT IN ('SUCCEEDED','FAILED','CANCELLED') OR lease_owner IS NULL)
);

CREATE TABLE forest_deployment.provisioning_operations (
    operation_id text PRIMARY KEY CHECK (octet_length(operation_id) BETWEEN 1 AND 256),
    operation_kind text NOT NULL CHECK (operation_kind IN ('ENQUEUE','CANCEL','RECONCILE')),
    input_digest text NOT NULL CHECK (input_digest ~ '^sha256:[a-f0-9]{64}$'),
    job_id text NOT NULL REFERENCES forest_deployment.provisioning_jobs(job_id),
    resulting_revision bigint NOT NULL CHECK (resulting_revision > 0),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp()
);

CREATE TABLE forest_deployment.bots (
    bot_id text PRIMARY KEY CHECK (octet_length(bot_id) BETWEEN 1 AND 128),
    display_name text NOT NULL CHECK (octet_length(display_name) BETWEEN 1 AND 256),
    supply_state text NOT NULL CHECK (supply_state IN ('READY','DELETING','DELETED')),
    storage_profile_ref text NOT NULL CHECK (octet_length(storage_profile_ref) BETWEEN 1 AND 512),
    storage_generation bigint NOT NULL CHECK (storage_generation > 0),
    bootstrap_profile_ref text NOT NULL CHECK (
        octet_length(bootstrap_profile_ref) BETWEEN 1 AND 512
    ),
    bootstrap_manifest_digest text NOT NULL CHECK (
        bootstrap_manifest_digest ~ '^sha256:[a-f0-9]{64}$'
    ),
    revision bigint NOT NULL DEFAULT 1 CHECK (revision > 0),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    FOREIGN KEY (storage_profile_ref,bot_id,storage_generation)
        REFERENCES forest_deployment.storage_profiles(profile_ref,bot_id,storage_generation),
    FOREIGN KEY (
        bootstrap_profile_ref,bot_id,storage_profile_ref,storage_generation,
        bootstrap_manifest_digest
    ) REFERENCES forest_deployment.bootstrap_profiles(
        profile_ref,bot_id,storage_profile_ref,storage_generation,manifest_digest
    )
);

CREATE TABLE forest_deployment.runtime_intents (
    bot_id text PRIMARY KEY REFERENCES forest_deployment.bots(bot_id),
    desired_state text NOT NULL CHECK (desired_state IN ('OFFLINE','ONLINE')),
    revision bigint NOT NULL DEFAULT 1 CHECK (revision > 0),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp()
);

CREATE TABLE forest_deployment.runtime_observations (
    bot_id text PRIMARY KEY REFERENCES forest_deployment.bots(bot_id),
    observed_state text NOT NULL CHECK (
        observed_state IN ('OFFLINE','STARTING','ONLINE','STOPPING','FAILED')
    ),
    supervisor_owner_epoch bigint NOT NULL CHECK (supervisor_owner_epoch > 0),
    supervisor_incarnation text NOT NULL,
    graph_id text,
    error_code text CHECK (error_code IS NULL OR error_code ~ '^[A-Z][A-Z0-9_]{0,127}$'),
    consecutive_failures integer NOT NULL DEFAULT 0 CHECK (consecutive_failures >= 0),
    retry_not_before timestamptz,
    revision bigint NOT NULL DEFAULT 1 CHECK (revision > 0),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    CHECK ((observed_state='FAILED') = (error_code IS NOT NULL)),
    CHECK ((observed_state='FAILED') = (retry_not_before IS NOT NULL)),
    CHECK (observed_state<>'ONLINE' OR graph_id IS NOT NULL)
);

CREATE TABLE forest_deployment.operations (
    operation_id text PRIMARY KEY CHECK (octet_length(operation_id) BETWEEN 1 AND 256),
    operation_kind text NOT NULL CHECK (operation_kind IN ('REGISTER_READY','SET_DESIRED')),
    input_digest text NOT NULL CHECK (input_digest ~ '^sha256:[a-f0-9]{64}$'),
    bot_id text NOT NULL REFERENCES forest_deployment.bots(bot_id),
    resulting_revision bigint NOT NULL CHECK (resulting_revision > 0),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp()
);

-- The kernel owns Bot identity and runtime intent. Official deployment packages
-- may only project this state or submit an authenticated intent change here.
CREATE FUNCTION forest_deployment.bot_catalog_page(
    requested_limit integer, after_bot_id text, search_text text
) RETURNS jsonb LANGUAGE plpgsql STABLE SET search_path = pg_catalog AS $$
DECLARE
    entries jsonb;
    found_count integer;
BEGIN
    IF requested_limit NOT BETWEEN 1 AND 100
       OR (after_bot_id IS NOT NULL AND (octet_length(after_bot_id) NOT BETWEEN 1 AND 128))
       OR (search_text IS NOT NULL AND octet_length(search_text) > 128) THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object('code','BOT_CATALOG_QUERY_INVALID',
                'message','Bot catalog query is invalid','status',422));
    END IF;
    SELECT COALESCE(jsonb_agg(jsonb_build_object(
        'bot_id',page.bot_id,'display_name',page.display_name,
        'supply_state',page.supply_state,'desired_state',page.desired_state,
        'intent_revision',page.intent_revision,'observed_state',page.observed_state,
        'error_code',page.error_code) ORDER BY page.bot_id),'[]'::jsonb),count(*)
    INTO entries,found_count
    FROM (
        SELECT b.bot_id,b.display_name,b.supply_state,i.desired_state,
            i.revision AS intent_revision,o.observed_state,o.error_code
        FROM forest_deployment.bots b
        JOIN forest_deployment.runtime_intents i USING(bot_id)
        LEFT JOIN forest_deployment.runtime_observations o USING(bot_id)
        WHERE (after_bot_id IS NULL OR b.bot_id>after_bot_id)
          AND (search_text IS NULL OR search_text='' OR
               b.bot_id ILIKE '%'||search_text||'%' OR
               b.display_name ILIKE '%'||search_text||'%')
        ORDER BY b.bot_id LIMIT requested_limit+1
    ) page;
    IF found_count>requested_limit THEN
        entries:=entries-requested_limit;
    END IF;
    RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',true,
        'value',jsonb_build_object('items',entries,
            'next_after',CASE WHEN found_count>requested_limit
                THEN entries->(requested_limit-1)->>'bot_id' ELSE NULL END));
END;
$$;

CREATE FUNCTION forest_deployment.bot_set_desired(
    requested_operation text, requested_digest text, requested_bot text,
    requested_state text, expected_revision bigint,
    caller_component text, caller_generation text
) RETURNS jsonb LANGUAGE plpgsql VOLATILE SET search_path = pg_catalog AS $$
DECLARE
    prior forest_deployment.operations%ROWTYPE;
    current_revision bigint;
    saved_revision bigint;
BEGIN
    IF octet_length(requested_operation) NOT BETWEEN 1 AND 256
       OR requested_digest !~ '^sha256:[a-f0-9]{64}$'
       OR octet_length(requested_bot) NOT BETWEEN 1 AND 128
       OR requested_state NOT IN ('ONLINE','OFFLINE')
       OR expected_revision IS NULL OR expected_revision<1 THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object('code','DEPLOYMENT_INPUT_INVALID',
                'message','Runtime intent is invalid','status',422));
    END IF;
    IF caller_component IS NOT NULL THEN
        IF caller_generation IS NULL OR NOT EXISTS (
            SELECT 1 FROM forest_deployment.web_authority_receipts r
            WHERE r.target_operation_ref=requested_operation
              AND r.target_component_id=caller_component
              AND r.target_generation_id=caller_generation
              AND r.action_key='bot.set-desired'
              AND r.input_digest=requested_digest AND r.web_role='ADMIN'
        ) THEN
            RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
                'error',jsonb_build_object('code','WEB_AUTHORITY_TICKET_REQUIRED',
                    'message','Bot intent change requires an accepted Web ticket','status',403));
        END IF;
    END IF;
    PERFORM pg_advisory_xact_lock(hashtextextended(
        'forest.deployment.operation/'||requested_operation,0));
    SELECT * INTO prior FROM forest_deployment.operations
        WHERE operation_id=requested_operation;
    IF FOUND THEN
        IF prior.input_digest<>requested_digest OR prior.operation_kind<>'SET_DESIRED' THEN
            RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
                'error',jsonb_build_object('code','DEPLOYMENT_OPERATION_CONFLICT',
                    'message','Operation input differs','status',409));
        END IF;
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',true,
            'value',jsonb_build_object('bot_id',prior.bot_id,
                'intent_revision',prior.resulting_revision));
    END IF;
    SELECT revision INTO current_revision FROM forest_deployment.runtime_intents
        WHERE bot_id=requested_bot FOR UPDATE;
    IF NOT FOUND THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object('code','BOT_NOT_FOUND',
                'message','Bot identity does not exist','status',404));
    END IF;
    IF current_revision<>expected_revision THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object('code','BOT_INTENT_REVISION_CONFLICT',
                'message','Bot runtime intent changed','status',409));
    END IF;
    UPDATE forest_deployment.runtime_intents SET desired_state=requested_state,
        revision=revision+1,updated_at=clock_timestamp()
        WHERE bot_id=requested_bot RETURNING revision INTO saved_revision;
    INSERT INTO forest_deployment.operations(
        operation_id,operation_kind,input_digest,bot_id,resulting_revision
    ) VALUES (requested_operation,'SET_DESIRED',requested_digest,
        requested_bot,saved_revision);
    RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',true,
        'value',jsonb_build_object('bot_id',requested_bot,
            'intent_revision',saved_revision));
END;
$$;

CREATE TABLE forest_deployment.secret_slots (
    secret_ref text PRIMARY KEY CHECK (octet_length(secret_ref) BETWEEN 10 AND 512),
    purpose text NOT NULL CHECK (octet_length(purpose) BETWEEN 1 AND 256),
    current_version integer NOT NULL CHECK (current_version > 0),
    revision bigint NOT NULL DEFAULT 1 CHECK (revision > 0),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp()
);

CREATE TABLE forest_deployment.secret_versions (
    secret_ref text NOT NULL REFERENCES forest_deployment.secret_slots(secret_ref),
    version integer NOT NULL CHECK (version > 0),
    operation_id text NOT NULL UNIQUE CHECK (octet_length(operation_id) BETWEEN 1 AND 256),
    input_digest text NOT NULL CHECK (input_digest ~ '^sha256:[a-f0-9]{64}$'),
    key_id text NOT NULL CHECK (octet_length(key_id) BETWEEN 1 AND 256),
    nonce bytea NOT NULL CHECK (octet_length(nonce)=12),
    ciphertext bytea NOT NULL CHECK (octet_length(ciphertext) BETWEEN 17 AND 65552),
    plaintext_digest text NOT NULL CHECK (plaintext_digest ~ '^sha256:[a-f0-9]{64}$'),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (secret_ref,version)
);

CREATE FUNCTION forest_deployment.secret_metadata_page(
    requested_limit integer, after_secret_ref text, search_text text
) RETURNS jsonb LANGUAGE plpgsql STABLE SET search_path = pg_catalog AS $$
DECLARE
    entries jsonb;
    found_count integer;
BEGIN
    IF requested_limit NOT BETWEEN 1 AND 100
       OR (after_secret_ref IS NOT NULL AND
           octet_length(after_secret_ref) NOT BETWEEN 1 AND 512)
       OR (search_text IS NOT NULL AND octet_length(search_text)>128) THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object('code','SECRET_QUERY_INVALID',
                'message','Secret metadata query is invalid','status',422));
    END IF;
    SELECT COALESCE(jsonb_agg(jsonb_build_object(
        'secret_ref',page.secret_ref,'purpose',page.purpose,
        'version',page.current_version,'revision',page.revision,
        'key_id',page.key_id) ORDER BY page.secret_ref),'[]'::jsonb),count(*)
    INTO entries,found_count
    FROM (
        SELECT s.secret_ref,s.purpose,s.current_version,s.revision,v.key_id
        FROM forest_deployment.secret_slots s
        JOIN forest_deployment.secret_versions v ON
            v.secret_ref=s.secret_ref AND v.version=s.current_version
        WHERE (after_secret_ref IS NULL OR s.secret_ref>after_secret_ref)
          AND (search_text IS NULL OR search_text='' OR
               s.secret_ref ILIKE '%'||search_text||'%' OR
               s.purpose ILIKE '%'||search_text||'%')
        ORDER BY s.secret_ref LIMIT requested_limit+1
    ) page;
    IF found_count>requested_limit THEN
        entries:=entries-requested_limit;
    END IF;
    RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',true,
        'value',jsonb_build_object('items',entries,
            'next_after',CASE WHEN found_count>requested_limit
                THEN entries->(requested_limit-1)->>'secret_ref' ELSE NULL END));
END;
$$;

CREATE INDEX bots_supply_idx ON forest_deployment.bots(supply_state,created_at,bot_id);
CREATE INDEX runtime_intents_online_browse_idx
    ON forest_deployment.runtime_intents(bot_id) WHERE desired_state='ONLINE';
CREATE INDEX observations_retry_idx
    ON forest_deployment.runtime_observations(observed_state,retry_not_before,bot_id);
CREATE INDEX provisioning_claim_idx
    ON forest_deployment.provisioning_jobs(state,retry_not_before,created_at,job_id);
CREATE INDEX provisioning_expired_running_idx
    ON forest_deployment.provisioning_jobs(lease_expires_at,job_id)
    WHERE state='RUNNING';
CREATE INDEX provisioning_exhausted_retry_idx
    ON forest_deployment.provisioning_jobs(retry_not_before,job_id)
    WHERE state='RETRY_WAIT' AND attempt_no>=max_attempts;

INSERT INTO forest_deployment.supervisor_lease(singleton) VALUES (true);

CREATE FUNCTION forest_deployment.reject_identity_change() RETURNS trigger LANGUAGE plpgsql AS '
BEGIN
    RAISE EXCEPTION ''deployment identity is immutable'';
END;
';

CREATE TRIGGER deployment_identity_immutable
    BEFORE UPDATE OR DELETE ON forest_deployment.deployment_identity
    FOR EACH ROW EXECUTE FUNCTION forest_deployment.reject_identity_change();

CREATE TRIGGER deployment_operation_immutable
    BEFORE UPDATE OR DELETE ON forest_deployment.operations
    FOR EACH ROW EXECUTE FUNCTION forest_deployment.reject_identity_change();

CREATE TRIGGER web_account_operation_immutable
    BEFORE UPDATE OR DELETE ON forest_deployment.web_account_operations
    FOR EACH ROW EXECUTE FUNCTION forest_deployment.reject_identity_change();

CREATE FUNCTION forest_deployment.guard_web_setup_change() RETURNS trigger LANGUAGE plpgsql AS '
BEGIN
    IF TG_OP = ''DELETE'' OR OLD.state <> ''PENDING''
       OR ROW(NEW.deployment_id,NEW.setup_id,NEW.created_at)
          IS DISTINCT FROM ROW(OLD.deployment_id,OLD.setup_id,OLD.created_at)
       OR NEW.revision <> OLD.revision + 1 THEN
        RAISE EXCEPTION ''invalid Web setup lifecycle transition'';
    END IF;
    IF NEW.state = ''PENDING'' AND (
        clock_timestamp() < OLD.expires_at
        OR NEW.issued_at < OLD.expires_at
        OR NEW.pairing_code_digest IS NOT DISTINCT FROM OLD.pairing_code_digest
    ) THEN
        RAISE EXCEPTION ''invalid Web setup credential rotation'';
    ELSIF NEW.state = ''COMPLETED'' AND
        ROW(NEW.issued_at,NEW.expires_at)
        IS DISTINCT FROM ROW(OLD.issued_at,OLD.expires_at) THEN
        RAISE EXCEPTION ''invalid Web setup completion'';
    ELSIF NEW.state NOT IN (''PENDING'',''COMPLETED'') THEN
        RAISE EXCEPTION ''invalid Web setup target state'';
    END IF;
    RETURN NEW;
END;
';

CREATE TRIGGER web_setup_change_guard
    BEFORE UPDATE OR DELETE ON forest_deployment.web_setup_state
    FOR EACH ROW EXECUTE FUNCTION forest_deployment.guard_web_setup_change();

CREATE TRIGGER web_authority_receipt_immutable
    BEFORE UPDATE OR DELETE ON forest_deployment.web_authority_receipts
    FOR EACH ROW EXECUTE FUNCTION forest_deployment.reject_identity_change();

CREATE FUNCTION forest_deployment.guard_runtime_graph_change() RETURNS trigger
LANGUAGE plpgsql SET search_path = pg_catalog AS $$
BEGIN
    IF TG_OP = 'DELETE' THEN
        RAISE EXCEPTION 'Deployment Graph change request cannot be deleted';
    END IF;
    IF ROW(OLD.deployment_id,OLD.operation_id,OLD.operation_kind,OLD.input_digest,OLD.plan_digest,OLD.requested_by,
           OLD.target_component_id,OLD.package_digest,OLD.configuration_revision,
           OLD.configuration_payload_digest,OLD.expected_graph_revision,
           OLD.activation_id,OLD.receipt_ticket_id,OLD.created_at)
       IS DISTINCT FROM
       ROW(NEW.deployment_id,NEW.operation_id,NEW.operation_kind,NEW.input_digest,NEW.plan_digest,NEW.requested_by,
           NEW.target_component_id,NEW.package_digest,NEW.configuration_revision,
           NEW.configuration_payload_digest,NEW.expected_graph_revision,
           NEW.activation_id,NEW.receipt_ticket_id,NEW.created_at)
       OR OLD.state IN ('SUCCEEDED','FAILED')
       OR NOT (
           (OLD.state='QUEUED' AND NEW.state='RUNNING'
                AND NEW.attempt_count=OLD.attempt_count+1 AND NEW.owner_epoch>0)
           OR (OLD.state='RUNNING' AND NEW.state='RUNNING'
                AND NEW.attempt_count=OLD.attempt_count+1
                AND NEW.owner_epoch>OLD.owner_epoch)
           OR (OLD.state='RUNNING' AND NEW.state IN ('SUCCEEDED','FAILED')
                AND NEW.attempt_count=OLD.attempt_count
                AND NEW.owner_epoch=OLD.owner_epoch)
       ) OR NEW.updated_at<OLD.updated_at THEN
        RAISE EXCEPTION 'Invalid deployment Graph change transition';
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER runtime_graph_change_transition
    BEFORE UPDATE OR DELETE ON forest_deployment.runtime_graph_change_requests
    FOR EACH ROW EXECUTE FUNCTION forest_deployment.guard_runtime_graph_change();

CREATE TRIGGER deployment_audit_record_immutable
    BEFORE UPDATE OR DELETE ON forest_deployment.audit_records
    FOR EACH ROW EXECUTE FUNCTION forest_deployment.reject_identity_change();

CREATE TRIGGER deployment_secret_version_immutable
    BEFORE UPDATE OR DELETE ON forest_deployment.secret_versions
    FOR EACH ROW EXECUTE FUNCTION forest_deployment.reject_identity_change();

CREATE TRIGGER deployment_storage_profile_immutable
    BEFORE UPDATE OR DELETE ON forest_deployment.storage_profiles
    FOR EACH ROW EXECUTE FUNCTION forest_deployment.reject_identity_change();

CREATE TRIGGER deployment_storage_request_immutable
    BEFORE UPDATE OR DELETE ON forest_deployment.storage_requests
    FOR EACH ROW EXECUTE FUNCTION forest_deployment.reject_identity_change();

CREATE TRIGGER deployment_storage_resource_immutable
    BEFORE UPDATE OR DELETE ON forest_deployment.storage_request_resources
    FOR EACH ROW EXECUTE FUNCTION forest_deployment.reject_identity_change();

CREATE TRIGGER deployment_storage_pin_immutable
    BEFORE UPDATE OR DELETE ON forest_deployment.storage_request_pins
    FOR EACH ROW EXECUTE FUNCTION forest_deployment.reject_identity_change();

CREATE TRIGGER deployment_storage_profile_operation_immutable
    BEFORE UPDATE OR DELETE ON forest_deployment.storage_profile_operations
    FOR EACH ROW EXECUTE FUNCTION forest_deployment.reject_identity_change();

CREATE TRIGGER deployment_bootstrap_profile_immutable
    BEFORE UPDATE OR DELETE ON forest_deployment.bootstrap_profiles
    FOR EACH ROW EXECUTE FUNCTION forest_deployment.reject_identity_change();

CREATE TRIGGER deployment_bootstrap_component_immutable
    BEFORE UPDATE OR DELETE ON forest_deployment.bootstrap_profile_components
    FOR EACH ROW EXECUTE FUNCTION forest_deployment.reject_identity_change();

CREATE TRIGGER deployment_bootstrap_operation_immutable
    BEFORE UPDATE OR DELETE ON forest_deployment.bootstrap_profile_operations
    FOR EACH ROW EXECUTE FUNCTION forest_deployment.reject_identity_change();

CREATE TRIGGER deployment_provisioning_operation_immutable
    BEFORE UPDATE OR DELETE ON forest_deployment.provisioning_operations
    FOR EACH ROW EXECUTE FUNCTION forest_deployment.reject_identity_change();

CREATE FUNCTION forest_deployment.guard_provisioning_job_change() RETURNS trigger LANGUAGE plpgsql AS '
BEGIN
    IF TG_OP = ''DELETE'' OR ROW(
        NEW.job_id,NEW.bot_id,NEW.display_name,NEW.storage_profile_ref,NEW.storage_generation,
        NEW.bootstrap_profile_ref,NEW.bootstrap_manifest_digest,NEW.max_attempts,NEW.created_at
    ) IS DISTINCT FROM ROW(
        OLD.job_id,OLD.bot_id,OLD.display_name,OLD.storage_profile_ref,OLD.storage_generation,
        OLD.bootstrap_profile_ref,OLD.bootstrap_manifest_digest,OLD.max_attempts,OLD.created_at
    ) THEN
        RAISE EXCEPTION ''provisioning job identity is immutable'';
    END IF;
    IF ROW(
        NEW.state,NEW.attempt_no,NEW.lease_owner,NEW.lease_epoch,NEW.retry_not_before,
        NEW.last_error_code,NEW.completed_at
    ) IS NOT DISTINCT FROM ROW(
        OLD.state,OLD.attempt_no,OLD.lease_owner,OLD.lease_epoch,OLD.retry_not_before,
        OLD.last_error_code,OLD.completed_at
    ) THEN
        IF NEW.revision <> OLD.revision THEN
            RAISE EXCEPTION ''provisioning lease renewal cannot advance revision'';
        END IF;
    ELSIF NEW.revision <> OLD.revision + 1 THEN
        RAISE EXCEPTION ''provisioning job revision must advance once'';
    END IF;
    RETURN NEW;
END;
';

CREATE TRIGGER provisioning_job_change_guard
    BEFORE UPDATE OR DELETE ON forest_deployment.provisioning_jobs
    FOR EACH ROW EXECUTE FUNCTION forest_deployment.guard_provisioning_job_change();

CREATE FUNCTION forest_deployment.guard_bot_change() RETURNS trigger LANGUAGE plpgsql AS '
BEGIN
    IF TG_OP = ''DELETE'' THEN
        RAISE EXCEPTION ''bot identity cannot be deleted'';
    END IF;
    IF ROW(
        NEW.bot_id,NEW.storage_profile_ref,NEW.storage_generation,NEW.bootstrap_profile_ref,
        NEW.bootstrap_manifest_digest,NEW.created_at
    ) IS DISTINCT FROM ROW(
        OLD.bot_id,OLD.storage_profile_ref,OLD.storage_generation,OLD.bootstrap_profile_ref,
        OLD.bootstrap_manifest_digest,OLD.created_at
    ) THEN
        RAISE EXCEPTION ''bot identity is immutable'';
    END IF;
    IF OLD.supply_state = ''DELETED'' THEN
        RAISE EXCEPTION ''deleted bot identity cannot be reused'';
    END IF;
    IF NEW.revision <> OLD.revision + 1 THEN
        RAISE EXCEPTION ''bot revision must advance once'';
    END IF;
    RETURN NEW;
END;
';

CREATE TRIGGER bot_change_guard
    BEFORE UPDATE OR DELETE ON forest_deployment.bots
    FOR EACH ROW EXECUTE FUNCTION forest_deployment.guard_bot_change();

CREATE FUNCTION forest_deployment.guard_revisioned_change() RETURNS trigger LANGUAGE plpgsql AS '
BEGIN
    IF TG_OP = ''DELETE'' OR NEW.bot_id IS DISTINCT FROM OLD.bot_id THEN
        RAISE EXCEPTION ''revisioned deployment fact identity is immutable'';
    END IF;
    IF NEW.revision <> OLD.revision + 1 THEN
        RAISE EXCEPTION ''deployment fact revision must advance once'';
    END IF;
    RETURN NEW;
END;
';

CREATE TRIGGER runtime_intent_change_guard
    BEFORE UPDATE OR DELETE ON forest_deployment.runtime_intents
    FOR EACH ROW EXECUTE FUNCTION forest_deployment.guard_revisioned_change();

CREATE TRIGGER runtime_observation_change_guard
    BEFORE UPDATE OR DELETE ON forest_deployment.runtime_observations
    FOR EACH ROW EXECUTE FUNCTION forest_deployment.guard_revisioned_change();

CREATE FUNCTION forest_deployment.guard_secret_slot_change() RETURNS trigger LANGUAGE plpgsql AS '
BEGIN
    IF TG_OP = ''DELETE'' OR ROW(NEW.secret_ref,NEW.purpose,NEW.created_at)
       IS DISTINCT FROM ROW(OLD.secret_ref,OLD.purpose,OLD.created_at) THEN
        RAISE EXCEPTION ''secret slot identity is immutable'';
    END IF;
    IF NEW.revision <> OLD.revision + 1 OR NEW.current_version <> OLD.current_version + 1 THEN
        RAISE EXCEPTION ''secret slot version must advance once'';
    END IF;
    RETURN NEW;
END;
';

CREATE TRIGGER secret_slot_change_guard
    BEFORE UPDATE OR DELETE ON forest_deployment.secret_slots
    FOR EACH ROW EXECUTE FUNCTION forest_deployment.guard_secret_slot_change();

CREATE FUNCTION forest_deployment.guard_web_account_change() RETURNS trigger LANGUAGE plpgsql AS '
BEGIN
    IF TG_OP = ''DELETE'' OR ROW(NEW.subject_id,NEW.username,NEW.created_at)
       IS DISTINCT FROM ROW(OLD.subject_id,OLD.username,OLD.created_at) THEN
        RAISE EXCEPTION ''web account identity is immutable'';
    END IF;
    IF NEW.revision <> OLD.revision + 1 THEN
        RAISE EXCEPTION ''web account revision must advance once'';
    END IF;
    RETURN NEW;
END;
';

CREATE TRIGGER web_account_change_guard
    BEFORE UPDATE OR DELETE ON forest_deployment.web_accounts
    FOR EACH ROW EXECUTE FUNCTION forest_deployment.guard_web_account_change();

CREATE FUNCTION forest_deployment.guard_web_session_change() RETURNS trigger LANGUAGE plpgsql AS '
BEGIN
    IF TG_OP = ''DELETE'' OR ROW(NEW.session_id_hash,NEW.csrf_hash,NEW.subject_id,
       NEW.authn_strength,NEW.created_at,NEW.expires_at)
       IS DISTINCT FROM ROW(OLD.session_id_hash,OLD.csrf_hash,OLD.subject_id,
       OLD.authn_strength,OLD.created_at,OLD.expires_at) THEN
        RAISE EXCEPTION ''web session identity is immutable'';
    END IF;
    IF NEW.revision <> OLD.revision + 1 OR OLD.revoked_at IS NOT NULL
       OR NEW.revoked_at IS NULL THEN
        RAISE EXCEPTION ''web session may only be revoked once'';
    END IF;
    RETURN NEW;
END;
';

CREATE TRIGGER web_session_change_guard
    BEFORE UPDATE OR DELETE ON forest_deployment.web_sessions
    FOR EACH ROW EXECUTE FUNCTION forest_deployment.guard_web_session_change();
