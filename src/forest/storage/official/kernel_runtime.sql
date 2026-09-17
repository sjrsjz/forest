CREATE TABLE runtime_state (
    bot_id text PRIMARY KEY REFERENCES forest_kernel.bot_identity(bot_id),
    storage_generation bigint NOT NULL DEFAULT 1 CHECK (storage_generation > 0),
    status text NOT NULL DEFAULT 'ACTIVE' CHECK (status IN ('ACTIVE', 'SEALED')),
    maintenance_epoch bigint NOT NULL DEFAULT 1 CHECK (maintenance_epoch > 0),
    policy_revision bigint NOT NULL DEFAULT 1 CHECK (policy_revision > 0),
    data_admission boolean NOT NULL DEFAULT true,
    control_admission boolean NOT NULL DEFAULT true
);

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

CREATE TABLE calls (
    bot_id text NOT NULL REFERENCES forest_kernel.bot_identity(bot_id),
    call_id text NOT NULL,
    operation_id text NOT NULL,
    component_id text NOT NULL,
    generation_id text NOT NULL,
    route_epoch bigint NOT NULL CHECK (route_epoch >= 0),
    admission_lane text NOT NULL CHECK (admission_lane IN ('DATA', 'CONTROL')),
    service_name text,
    worker_incarnation text NOT NULL,
    host_id text NOT NULL,
    host_incarnation text NOT NULL,
    owner_epoch bigint NOT NULL CHECK (owner_epoch > 0),
    storage_generation bigint NOT NULL CHECK (storage_generation > 0),
    maintenance_epoch bigint NOT NULL CHECK (maintenance_epoch > 0),
    policy_revision bigint NOT NULL CHECK (policy_revision > 0),
    cancel_epoch bigint NOT NULL DEFAULT 0 CHECK (cancel_epoch >= 0),
    deadline_at timestamptz NOT NULL,
    input_payload_id text NOT NULL,
    input_digest text NOT NULL CHECK (input_digest ~ '^sha256:[a-f0-9]{64}$'),
    causal_root_kind text NOT NULL CHECK (causal_root_kind <> '' AND length(causal_root_kind) <= 128),
    causal_root_id text NOT NULL CHECK (causal_root_id <> '' AND length(causal_root_id) <= 256),
    parent_call_id text,
    root_call_id text NOT NULL,
    call_depth integer NOT NULL DEFAULT 0 CHECK (call_depth BETWEEN 0 AND 32),
    dependency_components text[] NOT NULL DEFAULT '{}',
    status text NOT NULL DEFAULT 'ADMITTED' CHECK (
        status IN ('ADMITTED', 'COMPLETED', 'FAILED', 'CANCELLED')
    ),
    result_payload_id text,
    result_digest text,
    error_code text,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (bot_id, call_id),
    UNIQUE (bot_id, operation_id),
    FOREIGN KEY (bot_id, generation_id, component_id)
        REFERENCES generations(bot_id, generation_id, component_id),
    FOREIGN KEY (bot_id, parent_call_id) REFERENCES calls(bot_id, call_id),
    FOREIGN KEY (bot_id, root_call_id) REFERENCES calls(bot_id, call_id),
    CHECK (
        (parent_call_id IS NULL AND root_call_id = call_id AND call_depth = 0)
        OR (parent_call_id IS NOT NULL AND root_call_id <> call_id AND call_depth > 0)
    ),
    CHECK ((result_payload_id IS NULL) = (result_digest IS NULL)),
    CHECK (status <> 'COMPLETED' OR result_payload_id IS NOT NULL),
    CHECK ((admission_lane = 'DATA' AND service_name IS NULL) OR service_name IS NOT NULL)
);

CREATE INDEX calls_drain ON calls(bot_id, component_id, status);
CREATE INDEX calls_parent ON calls(bot_id, parent_call_id);
CREATE INDEX calls_root ON calls(bot_id, root_call_id, call_depth, call_id);
CREATE INDEX calls_causal_root ON calls(
    bot_id, causal_root_kind, causal_root_id, component_id, generation_id, status
);

CREATE TABLE state_heads (
    bot_id text NOT NULL REFERENCES forest_kernel.bot_identity(bot_id),
    component_id text NOT NULL,
    state_key text NOT NULL,
    revision bigint NOT NULL CHECK (revision >= 0),
    deleted boolean NOT NULL DEFAULT false,
    value_payload_id text,
    value_digest text,
    PRIMARY KEY (bot_id, component_id, state_key),
    CHECK ((value_payload_id IS NULL) = (value_digest IS NULL)),
    CHECK (revision = 0 OR deleted OR value_payload_id IS NOT NULL)
);

CREATE TABLE state_steps (
    bot_id text NOT NULL,
    call_id text NOT NULL,
    operation_id text NOT NULL,
    input_digest text NOT NULL CHECK (input_digest ~ '^sha256:[a-f0-9]{64}$'),
    value_payload_id text NOT NULL,
    value_digest text NOT NULL CHECK (value_digest ~ '^sha256:[a-f0-9]{64}$'),
    receipt_payload_id text NOT NULL,
    receipt_digest text NOT NULL CHECK (receipt_digest ~ '^sha256:[a-f0-9]{64}$'),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (bot_id, call_id, operation_id),
    FOREIGN KEY (bot_id, call_id) REFERENCES calls(bot_id, call_id)
);

CREATE TABLE state_revisions (
    bot_id text NOT NULL,
    component_id text NOT NULL,
    state_key text NOT NULL,
    revision bigint NOT NULL,
    previous_revision bigint NOT NULL CHECK (previous_revision = revision - 1),
    deleted boolean NOT NULL DEFAULT false,
    value_payload_id text,
    value_digest text,
    call_id text NOT NULL,
    operation_id text NOT NULL,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (bot_id, component_id, state_key, revision),
    FOREIGN KEY (bot_id, call_id, operation_id)
        REFERENCES state_steps(bot_id, call_id, operation_id),
    CHECK ((value_payload_id IS NULL) = (value_digest IS NULL)),
    CHECK (deleted OR value_payload_id IS NOT NULL)
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
    state text NOT NULL CHECK (state IN ('PREPARED', 'ACTIVE', 'RETIRED', 'FAILED', 'STOPPED')),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (bot_id, graph_id),
    UNIQUE (bot_id, graph_revision),
    UNIQUE (bot_id, activation_id)
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
    trust_level text NOT NULL CHECK (trust_level IN ('official', 'installed')),
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
            AND storage_repository_contract IS NULL AND storage_execution_profile IS NULL)
        OR (storage_schema_id IS NOT NULL AND storage_schema_version IS NOT NULL
            AND storage_namespace IS NOT NULL AND storage_baseline_sha256 IS NOT NULL
            AND storage_repository_contract IS NOT NULL
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

CREATE FUNCTION call_identity_guard() RETURNS trigger
LANGUAGE plpgsql SET search_path FROM CURRENT AS $$
BEGIN
    IF ROW(
        OLD.bot_id, OLD.call_id, OLD.operation_id, OLD.component_id,
        OLD.generation_id, OLD.route_epoch, OLD.admission_lane, OLD.service_name,
        OLD.worker_incarnation, OLD.host_id, OLD.host_incarnation, OLD.owner_epoch,
        OLD.storage_generation, OLD.maintenance_epoch, OLD.policy_revision,
        OLD.deadline_at, OLD.input_payload_id, OLD.input_digest,
        OLD.causal_root_kind, OLD.causal_root_id, OLD.parent_call_id,
        OLD.root_call_id, OLD.call_depth, OLD.dependency_components, OLD.created_at
    ) IS DISTINCT FROM ROW(
        NEW.bot_id, NEW.call_id, NEW.operation_id, NEW.component_id,
        NEW.generation_id, NEW.route_epoch, NEW.admission_lane, NEW.service_name,
        NEW.worker_incarnation, NEW.host_id, NEW.host_incarnation, NEW.owner_epoch,
        NEW.storage_generation, NEW.maintenance_epoch, NEW.policy_revision,
        NEW.deadline_at, NEW.input_payload_id, NEW.input_digest,
        NEW.causal_root_kind, NEW.causal_root_id, NEW.parent_call_id,
        NEW.root_call_id, NEW.call_depth, NEW.dependency_components, NEW.created_at
    ) THEN
        RAISE EXCEPTION 'Forest call identity is immutable' USING ERRCODE = '55000';
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

CREATE TRIGGER host_incarnations_immutable BEFORE UPDATE OR DELETE ON host_incarnations
    FOR EACH ROW EXECUTE FUNCTION reject_fact_mutation();
CREATE TRIGGER generation_identity BEFORE UPDATE ON generations
    FOR EACH ROW EXECUTE FUNCTION generation_identity_guard();
CREATE TRIGGER call_identity BEFORE UPDATE ON calls
    FOR EACH ROW EXECUTE FUNCTION call_identity_guard();
CREATE TRIGGER runtime_graph_identity BEFORE UPDATE ON runtime_graphs
    FOR EACH ROW EXECUTE FUNCTION runtime_graph_identity_guard();
CREATE TRIGGER state_steps_immutable BEFORE UPDATE OR DELETE ON state_steps
    FOR EACH ROW EXECUTE FUNCTION reject_fact_mutation();
CREATE TRIGGER state_revisions_immutable BEFORE UPDATE OR DELETE ON state_revisions
    FOR EACH ROW EXECUTE FUNCTION reject_fact_mutation();
CREATE TRIGGER runtime_graph_modules_immutable BEFORE UPDATE OR DELETE ON runtime_graph_modules
    FOR EACH ROW EXECUTE FUNCTION reject_fact_mutation();
CREATE TRIGGER runtime_graph_dependencies_immutable BEFORE UPDATE OR DELETE ON runtime_graph_dependencies
    FOR EACH ROW EXECUTE FUNCTION reject_fact_mutation();
CREATE TRIGGER runtime_graph_lifecycle_facts_immutable
    BEFORE UPDATE OR DELETE ON runtime_graph_lifecycle_facts
    FOR EACH ROW EXECUTE FUNCTION reject_fact_mutation();

INSERT INTO runtime_state(bot_id) SELECT bot_id FROM forest_kernel.bot_identity;
INSERT INTO host_lease(bot_id) SELECT bot_id FROM forest_kernel.bot_identity;
INSERT INTO runtime_graph_clock(bot_id) SELECT bot_id FROM forest_kernel.bot_identity;
