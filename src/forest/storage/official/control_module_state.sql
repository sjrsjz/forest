CREATE TABLE __forest_module_schema__.component_state_contracts (
    __scope_id_column__ text NOT NULL REFERENCES __scope_schema__.__scope_identity_table__(__scope_id_column__),
    component_id text NOT NULL CHECK (component_id ~ '^[a-z0-9][a-z0-9._-]{0,127}$'),
    state_schema integer NOT NULL CHECK (state_schema > 0),
    contract_digest text NOT NULL CHECK (contract_digest ~ '^sha256:[a-f0-9]{64}$'),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (__scope_id_column__,component_id)
);

CREATE TABLE __forest_module_schema__.component_state_payloads (
    __scope_id_column__ text NOT NULL REFERENCES __scope_schema__.__scope_identity_table__(__scope_id_column__),
    component_id text NOT NULL CHECK (component_id ~ '^[a-z0-9][a-z0-9._-]{0,127}$'),
    payload_id text NOT NULL CHECK (octet_length(payload_id) BETWEEN 1 AND 128),
    operation_id text NOT NULL CHECK (octet_length(operation_id) BETWEEN 1 AND 256),
    content_digest text NOT NULL CHECK (content_digest ~ '^sha256:[a-f0-9]{64}$'),
    content_bytes bytea NOT NULL CHECK (octet_length(content_bytes) BETWEEN 1 AND 65536),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (__scope_id_column__,component_id,payload_id),
    UNIQUE (__scope_id_column__,component_id,operation_id),
    UNIQUE (__scope_id_column__,component_id,payload_id,content_digest),
    FOREIGN KEY (__scope_id_column__,component_id)
        REFERENCES __forest_module_schema__.component_state_contracts(__scope_id_column__,component_id),
    CHECK (content_digest='sha256:' || encode(sha256(content_bytes),'hex'))
);

CREATE TABLE __forest_module_schema__.component_state_steps (
    __scope_id_column__ text NOT NULL REFERENCES __scope_schema__.__scope_identity_table__(__scope_id_column__),
    component_id text NOT NULL,
    operation_id text NOT NULL CHECK (octet_length(operation_id) BETWEEN 1 AND 256),
    generation_id text NOT NULL,
    call_id text NOT NULL CHECK (octet_length(call_id) BETWEEN 1 AND 128),
    state_key text NOT NULL CHECK (octet_length(state_key) BETWEEN 1 AND 128),
    expected_revision bigint NOT NULL CHECK (expected_revision >= 0),
    revision bigint NOT NULL CHECK (revision > 0),
    input_digest text NOT NULL CHECK (input_digest ~ '^sha256:[a-f0-9]{64}$'),
    payload_id text NOT NULL,
    value_digest text NOT NULL,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (__scope_id_column__,component_id,operation_id),
    UNIQUE (__scope_id_column__,component_id,state_key,revision),
    UNIQUE (__scope_id_column__,component_id,state_key,revision,payload_id,value_digest),
    FOREIGN KEY (__scope_id_column__,generation_id)
        REFERENCES __scope_schema__.runtime_generations(__scope_id_column__,generation_id),
    FOREIGN KEY (__scope_id_column__,component_id,payload_id,value_digest)
        REFERENCES __forest_module_schema__.component_state_payloads(__scope_id_column__,component_id,payload_id,content_digest),
    CHECK (revision=expected_revision+1)
);

CREATE TABLE __forest_module_schema__.component_state_heads (
    __scope_id_column__ text NOT NULL REFERENCES __scope_schema__.__scope_identity_table__(__scope_id_column__),
    component_id text NOT NULL,
    state_key text NOT NULL,
    revision bigint NOT NULL CHECK (revision > 0),
    payload_id text NOT NULL,
    value_digest text NOT NULL,
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (__scope_id_column__,component_id,state_key),
    FOREIGN KEY (__scope_id_column__,component_id,state_key,revision,payload_id,value_digest)
        REFERENCES __forest_module_schema__.component_state_steps(
            __scope_id_column__,component_id,state_key,revision,payload_id,value_digest)
);

CREATE FUNCTION __forest_module_schema__.reject_component_state_fact_change() RETURNS trigger
LANGUAGE plpgsql SET search_path = pg_catalog AS $$
BEGIN
    RAISE EXCEPTION 'Component state fact is immutable' USING ERRCODE='55000';
END;
$$;

CREATE FUNCTION __forest_module_schema__.guard_component_state_head_change() RETURNS trigger
LANGUAGE plpgsql SET search_path = pg_catalog AS $$
BEGIN
    IF TG_OP='DELETE' THEN
        RAISE EXCEPTION 'Component state head cannot be deleted' USING ERRCODE='55000';
    END IF;
    IF OLD.__scope_id_column__ IS DISTINCT FROM NEW.__scope_id_column__
       OR OLD.component_id IS DISTINCT FROM NEW.component_id
       OR OLD.state_key IS DISTINCT FROM NEW.state_key
       OR NEW.revision<>OLD.revision+1 THEN
        RAISE EXCEPTION 'Component state head transition is invalid' USING ERRCODE='55000';
    END IF;
    NEW.updated_at := clock_timestamp();
    RETURN NEW;
END;
$$;

CREATE TRIGGER component_state_contract_immutable
    BEFORE UPDATE OR DELETE ON __forest_module_schema__.component_state_contracts
    FOR EACH ROW EXECUTE FUNCTION __forest_module_schema__.reject_component_state_fact_change();
CREATE TRIGGER component_state_payload_immutable
    BEFORE UPDATE OR DELETE ON __forest_module_schema__.component_state_payloads
    FOR EACH ROW EXECUTE FUNCTION __forest_module_schema__.reject_component_state_fact_change();
CREATE TRIGGER component_state_step_immutable
    BEFORE UPDATE OR DELETE ON __forest_module_schema__.component_state_steps
    FOR EACH ROW EXECUTE FUNCTION __forest_module_schema__.reject_component_state_fact_change();
CREATE TRIGGER component_state_head_transition
    BEFORE UPDATE OR DELETE ON __forest_module_schema__.component_state_heads
    FOR EACH ROW EXECUTE FUNCTION __forest_module_schema__.guard_component_state_head_change();
