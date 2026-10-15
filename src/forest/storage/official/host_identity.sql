CREATE SCHEMA forest_host;

CREATE TABLE forest_host.host_identity (
    singleton boolean PRIMARY KEY DEFAULT true CHECK (singleton),
    host_id text NOT NULL UNIQUE CHECK (host_id ~ '^host/[a-f0-9]{32}$'),
    baseline_sha256 text NOT NULL CHECK (baseline_sha256 ~ '^[a-f0-9]{64}$'),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp()
);

CREATE TABLE forest_host.schema_bundles (
    host_id text NOT NULL REFERENCES forest_host.host_identity(host_id),
    schema_id text NOT NULL,
    component_id text NOT NULL,
    schema_version text NOT NULL,
    scope text NOT NULL CHECK (scope = 'host'),
    namespace name NOT NULL UNIQUE,
    baseline_sha256 text NOT NULL CHECK (baseline_sha256 ~ '^[a-f0-9]{64}$'),
    package_digest text NOT NULL CHECK (package_digest ~ '^sha256:[a-f0-9]{64}$'),
    repository_contract text NOT NULL,
    execution_profile text NOT NULL CHECK (execution_profile IN ('declarative','official')),
    state text NOT NULL CHECK (state IN ('READY','DETACHED')),
    installed_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    detached_at timestamptz,
    PRIMARY KEY (host_id,schema_id),
    CHECK (
        (state='READY' AND detached_at IS NULL)
        OR (state='DETACHED' AND detached_at IS NOT NULL)
    )
);

CREATE TABLE forest_host.runtime_graph_clock (
    host_id text PRIMARY KEY REFERENCES forest_host.host_identity(host_id),
    next_revision bigint NOT NULL DEFAULT 0 CHECK (next_revision >= 0),
    active_revision bigint NOT NULL DEFAULT 0 CHECK (active_revision >= 0),
    active_graph_id text,
    CHECK (active_revision <= next_revision)
);

CREATE TABLE forest_host.runtime_graphs (
    host_id text NOT NULL REFERENCES forest_host.host_identity(host_id),
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
    PRIMARY KEY (host_id,graph_id),
    UNIQUE (host_id,graph_revision),
    UNIQUE (host_id,activation_id),
    FOREIGN KEY (host_id,source_graph_id)
        REFERENCES forest_host.runtime_graphs(host_id,graph_id)
);

CREATE TABLE forest_host.runtime_generations (
    host_id text NOT NULL REFERENCES forest_host.host_identity(host_id),
    generation_id text NOT NULL,
    component_id text NOT NULL,
    binding_digest text NOT NULL CHECK (binding_digest ~ '^sha256:[0-9a-f]{64}$'),
    binding jsonb NOT NULL,
    owner_epoch bigint NOT NULL CHECK (owner_epoch > 0),
    state text NOT NULL CHECK (state IN ('PREPARED','ACTIVE','QUIESCING','STOPPED','FAILED')),
    failure_code text CHECK (
        failure_code IS NULL OR failure_code ~ '^[A-Z][A-Z0-9_]{0,127}$'
    ),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (host_id,generation_id)
);

CREATE TABLE forest_host.runtime_graph_members (
    host_id text NOT NULL,
    graph_id text NOT NULL,
    component_id text NOT NULL,
    generation_id text NOT NULL,
    data_admission boolean NOT NULL,
    control_admission boolean NOT NULL,
    route_epoch bigint NOT NULL DEFAULT 1 CHECK (route_epoch > 0),
    PRIMARY KEY (host_id,graph_id,component_id),
    UNIQUE (host_id,graph_id,generation_id),
    FOREIGN KEY (host_id,graph_id)
        REFERENCES forest_host.runtime_graphs(host_id,graph_id),
    FOREIGN KEY (host_id,generation_id)
        REFERENCES forest_host.runtime_generations(host_id,generation_id)
);

ALTER TABLE forest_host.runtime_graph_clock
    ADD FOREIGN KEY (host_id,active_graph_id)
    REFERENCES forest_host.runtime_graphs(host_id,graph_id);

CREATE TABLE forest_host.runtime_package_selection (
    host_id text NOT NULL REFERENCES forest_host.host_identity(host_id),
    component_id text NOT NULL CHECK (component_id ~ '^[a-z0-9][a-z0-9._-]{0,127}$'),
    package_digest text NOT NULL CHECK (package_digest ~ '^sha256:[a-f0-9]{64}$'),
    configuration jsonb NOT NULL,
    graph_revision bigint NOT NULL CHECK (graph_revision > 0),
    PRIMARY KEY (host_id,component_id)
);

CREATE TABLE forest_host.package_service_operations (
    host_id text NOT NULL REFERENCES forest_host.host_identity(host_id),
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
    PRIMARY KEY (host_id,component_id,operation_id),
    UNIQUE (host_id,call_id),
    FOREIGN KEY (host_id,generation_id)
        REFERENCES forest_host.runtime_generations(host_id,generation_id),
    CHECK ((status='ACTIVE' AND outcome IS NULL AND completed_at IS NULL)
        OR (status='UNKNOWN' AND outcome IS NULL AND completed_at IS NOT NULL)
        OR (status IN ('SUCCEEDED','FAILED') AND outcome IS NOT NULL AND completed_at IS NOT NULL))
);
CREATE INDEX package_service_operations_status
    ON forest_host.package_service_operations(host_id,status,deadline_at);

CREATE TABLE forest_host.runtime_graph_change_requests (
    host_id text NOT NULL REFERENCES forest_host.host_identity(host_id),
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
        CHECK (state IN ('QUEUED','RUNNING','SUCCEEDED','FAILED','PARTIAL')),
    owner_epoch bigint CHECK (owner_epoch IS NULL OR owner_epoch > 0),
    attempt_count integer NOT NULL DEFAULT 0 CHECK (attempt_count >= 0),
    result_graph_id text,
    result_graph_revision bigint CHECK (result_graph_revision IS NULL OR result_graph_revision > 0),
    error_code text CHECK (error_code IS NULL OR error_code ~ '^[A-Z][A-Z0-9_]{0,127}$'),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (host_id,operation_id),
    UNIQUE (host_id,activation_id),
    FOREIGN KEY (host_id,result_graph_id)
        REFERENCES forest_host.runtime_graphs(host_id,graph_id),
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
        OR (state='PARTIAL' AND owner_epoch IS NOT NULL AND attempt_count>0
            AND result_graph_id IS NOT NULL AND result_graph_revision IS NOT NULL AND error_code IS NOT NULL)
    )
);
CREATE INDEX runtime_graph_change_queue
    ON forest_host.runtime_graph_change_requests(host_id,state,created_at,operation_id);

CREATE TABLE forest_host.driver_transition_consumers (
    host_id text NOT NULL REFERENCES forest_host.host_identity(host_id),
    host_activation_id text NOT NULL,
    bot_id text NOT NULL CHECK (octet_length(bot_id) BETWEEN 1 AND 128),
    bot_activation_id text NOT NULL CHECK (
        bot_activation_id ~ '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
    ),
    source_bot_graph_id text NOT NULL CHECK (octet_length(source_bot_graph_id) BETWEEN 1 AND 256),
    source_bot_graph_revision bigint NOT NULL CHECK (source_bot_graph_revision > 0),
    source_bindings jsonb NOT NULL CHECK (
        jsonb_typeof(source_bindings)='array' AND jsonb_array_length(source_bindings)>0
    ),
    target_bindings jsonb NOT NULL CHECK (
        jsonb_typeof(target_bindings)='array' AND jsonb_array_length(target_bindings)>0
    ),
    state text NOT NULL DEFAULT 'PENDING'
        CHECK (state IN ('PENDING','SUCCEEDED','FAILED','PARTIAL')),
    result_bot_graph_id text,
    result_bot_graph_revision bigint CHECK (
        result_bot_graph_revision IS NULL OR result_bot_graph_revision > 0
    ),
    error_code text CHECK (error_code IS NULL OR error_code ~ '^[A-Z][A-Z0-9_]{0,127}$'),
    prepared_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (host_id,host_activation_id,bot_id),
    UNIQUE (host_id,host_activation_id,bot_activation_id),
    FOREIGN KEY (host_id,host_activation_id)
        REFERENCES forest_host.runtime_graphs(host_id,activation_id),
    CHECK (
        (state='PENDING' AND result_bot_graph_id IS NULL
            AND result_bot_graph_revision IS NULL AND error_code IS NULL)
        OR (state='SUCCEEDED' AND result_bot_graph_id IS NOT NULL
            AND result_bot_graph_revision IS NOT NULL AND error_code IS NULL)
        OR (state='FAILED' AND result_bot_graph_id IS NULL
            AND result_bot_graph_revision IS NULL AND error_code IS NOT NULL)
        OR (state='PARTIAL' AND result_bot_graph_id IS NOT NULL
            AND result_bot_graph_revision IS NOT NULL AND error_code IS NOT NULL)
    )
);

CREATE TABLE forest_host.web_authority_receipts (
    host_id text NOT NULL REFERENCES forest_host.host_identity(host_id),
    ticket_id text NOT NULL CHECK (octet_length(ticket_id) BETWEEN 1 AND 256),
    ticket_digest text NOT NULL CHECK (ticket_digest ~ '^sha256:[a-f0-9]{64}$'),
    issuer_id text NOT NULL CHECK (octet_length(issuer_id) BETWEEN 1 AND 128),
    key_id text NOT NULL CHECK (octet_length(key_id) BETWEEN 1 AND 256),
    web_subject_ref text NOT NULL CHECK (octet_length(web_subject_ref) BETWEEN 1 AND 128),
    web_role text NOT NULL CHECK (web_role IN ('ADMIN','AUDITOR')),
    session_ref text NOT NULL CHECK (octet_length(session_ref) BETWEEN 1 AND 128),
    session_revision bigint NOT NULL CHECK (session_revision > 0),
    target_scope_kind text NOT NULL CHECK (target_scope_kind='host'),
    target_scope_id text NOT NULL CHECK (target_scope_id=host_id),
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
    PRIMARY KEY (host_id,ticket_id),
    UNIQUE (host_id,target_operation_ref),
    CHECK (expires_at > issued_at AND expires_at <= issued_at + interval '30 seconds')
);

ALTER TABLE forest_host.runtime_graph_change_requests
    ADD FOREIGN KEY (host_id,receipt_ticket_id)
    REFERENCES forest_host.web_authority_receipts(host_id,ticket_id);

CREATE TABLE forest_host.audit_records (
    host_id text NOT NULL REFERENCES forest_host.host_identity(host_id),
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
    resource_kind text NOT NULL CHECK (resource_kind='host.component'),
    resource_id text NOT NULL CHECK (octet_length(resource_id) BETWEEN 1 AND 128),
    operation_kind text NOT NULL CHECK (operation_kind IN ('web.action','web.query')),
    operation_id text NOT NULL CHECK (octet_length(operation_id) BETWEEN 1 AND 256),
    receipt_ticket_id text,
    CHECK ((operation_kind='web.action' AND receipt_ticket_id IS NOT NULL) OR
           (operation_kind='web.query' AND receipt_ticket_id IS NULL)),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (host_id,audit_id),
    UNIQUE (host_id,operation_id),
    FOREIGN KEY (host_id,receipt_ticket_id)
        REFERENCES forest_host.web_authority_receipts(host_id,ticket_id)
);
CREATE INDEX host_audit_time_idx
    ON forest_host.audit_records(host_id,created_at DESC,audit_id DESC);
CREATE INDEX host_audit_action_idx
    ON forest_host.audit_records(host_id,action,created_at DESC,audit_id DESC);
CREATE INDEX host_audit_actor_idx
    ON forest_host.audit_records(host_id,actor_kind,actor_id,created_at DESC);

CREATE FUNCTION forest_host.guard_runtime_graph_change() RETURNS trigger
LANGUAGE plpgsql SET search_path = pg_catalog AS $$
BEGIN
    IF TG_OP = 'DELETE' THEN
        RAISE EXCEPTION 'Host Graph change request cannot be deleted';
    END IF;
    IF ROW(OLD.host_id,OLD.operation_id,OLD.operation_kind,OLD.input_digest,OLD.plan_digest,OLD.requested_by,
           OLD.target_component_id,OLD.package_digest,OLD.configuration_revision,
           OLD.configuration_payload_digest,OLD.expected_graph_revision,
           OLD.activation_id,OLD.receipt_ticket_id,OLD.created_at)
       IS DISTINCT FROM
       ROW(NEW.host_id,NEW.operation_id,NEW.operation_kind,NEW.input_digest,NEW.plan_digest,NEW.requested_by,
           NEW.target_component_id,NEW.package_digest,NEW.configuration_revision,
           NEW.configuration_payload_digest,NEW.expected_graph_revision,
           NEW.activation_id,NEW.receipt_ticket_id,NEW.created_at)
       OR OLD.state IN ('SUCCEEDED','FAILED','PARTIAL')
       OR NOT (
           (OLD.state='QUEUED' AND NEW.state='RUNNING'
                AND NEW.attempt_count=OLD.attempt_count+1 AND NEW.owner_epoch>0)
           OR (OLD.state='RUNNING' AND NEW.state='RUNNING'
                AND NEW.attempt_count=OLD.attempt_count+1
                AND NEW.owner_epoch>OLD.owner_epoch)
           OR (OLD.state='RUNNING' AND NEW.state IN ('SUCCEEDED','FAILED','PARTIAL')
                AND NEW.attempt_count=OLD.attempt_count
                AND NEW.owner_epoch=OLD.owner_epoch)
       ) OR NEW.updated_at<OLD.updated_at THEN
        RAISE EXCEPTION 'Invalid host Graph change transition';
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER runtime_graph_change_transition
    BEFORE UPDATE OR DELETE ON forest_host.runtime_graph_change_requests
    FOR EACH ROW EXECUTE FUNCTION forest_host.guard_runtime_graph_change();

CREATE FUNCTION forest_host.guard_driver_transition_consumer() RETURNS trigger
LANGUAGE plpgsql SET search_path = pg_catalog AS $$
BEGIN
    IF TG_OP = 'DELETE' THEN
        RAISE EXCEPTION 'Host driver transition consumer cannot be deleted';
    END IF;
    IF OLD.state <> 'PENDING'
       OR ROW(OLD.host_id,OLD.host_activation_id,OLD.bot_id,OLD.bot_activation_id,
              OLD.source_bot_graph_id,OLD.source_bot_graph_revision,
              OLD.source_bindings,OLD.target_bindings,OLD.prepared_at)
          IS DISTINCT FROM
          ROW(NEW.host_id,NEW.host_activation_id,NEW.bot_id,NEW.bot_activation_id,
              NEW.source_bot_graph_id,NEW.source_bot_graph_revision,
              NEW.source_bindings,NEW.target_bindings,NEW.prepared_at)
       OR NEW.state NOT IN ('SUCCEEDED','FAILED','PARTIAL')
       OR (
           NEW.state IN ('SUCCEEDED','PARTIAL')
           AND NOT EXISTS (
               SELECT 1 FROM forest_host.runtime_graphs g
               WHERE g.host_id=OLD.host_id
                 AND g.activation_id=OLD.host_activation_id
                 AND g.state IN ('ACTIVE','RETIRED')
           )
       )
       OR NEW.updated_at < OLD.updated_at THEN
        RAISE EXCEPTION 'Invalid Host driver consumer transition';
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER driver_transition_consumer_terminal
    BEFORE UPDATE OR DELETE ON forest_host.driver_transition_consumers
    FOR EACH ROW EXECUTE FUNCTION forest_host.guard_driver_transition_consumer();

CREATE TABLE forest_host.supervisor_lease (
    singleton boolean NOT NULL DEFAULT true UNIQUE CHECK (singleton),
    host_id text PRIMARY KEY REFERENCES forest_host.host_identity(host_id),
    incarnation text CHECK (incarnation ~ '^incarnation/[0-9a-f-]{36}$'),
    owner_epoch bigint NOT NULL DEFAULT 0 CHECK (owner_epoch >= 0),
    expires_at timestamptz NOT NULL DEFAULT '1970-01-01 00:00:00+00'
);

CREATE FUNCTION forest_host.reject_identity_change() RETURNS trigger
LANGUAGE plpgsql SET search_path = pg_catalog AS $$
BEGIN
    RAISE EXCEPTION 'Host identity is immutable' USING ERRCODE = '55000';
END;
$$;

CREATE TRIGGER host_identity_immutable
BEFORE UPDATE OR DELETE ON forest_host.host_identity
FOR EACH ROW EXECUTE FUNCTION forest_host.reject_identity_change();

CREATE FUNCTION forest_host.reject_receipt_change() RETURNS trigger
LANGUAGE plpgsql SET search_path = pg_catalog AS $$
BEGIN
    RAISE EXCEPTION 'Host Web authority receipt is immutable' USING ERRCODE = '55000';
END;
$$;

CREATE TRIGGER web_authority_receipt_immutable
BEFORE UPDATE OR DELETE ON forest_host.web_authority_receipts
FOR EACH ROW EXECUTE FUNCTION forest_host.reject_receipt_change();

CREATE TRIGGER host_audit_record_immutable
BEFORE UPDATE OR DELETE ON forest_host.audit_records
FOR EACH ROW EXECUTE FUNCTION forest_host.reject_identity_change();
