CREATE TABLE module_configuration_heads (
    deployment_id text PRIMARY KEY REFERENCES forest_deployment.deployment_identity(deployment_id),
    desired_revision bigint NOT NULL DEFAULT 0 CHECK (desired_revision >= 0),
    effective_revision bigint NOT NULL DEFAULT 0 CHECK (effective_revision BETWEEN 0 AND desired_revision),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    CHECK ((desired_revision=0 AND effective_revision=0)
        OR (desired_revision>0 AND effective_revision>0))
);

CREATE TABLE module_configuration_plans (
    deployment_id text NOT NULL REFERENCES forest_deployment.deployment_identity(deployment_id),
    plan_id text NOT NULL CHECK (octet_length(plan_id) BETWEEN 1 AND 256),
    operation_id text NOT NULL CHECK (octet_length(operation_id) BETWEEN 1 AND 256),
    input_digest text NOT NULL CHECK (input_digest ~ '^sha256:[a-f0-9]{64}$'),
    expected_desired_revision bigint NOT NULL CHECK (expected_desired_revision >= 0),
    target_generation_id text CHECK (
        target_generation_id IS NULL OR octet_length(target_generation_id) BETWEEN 1 AND 256
    ),
    target_graph_revision bigint CHECK (target_graph_revision IS NULL OR target_graph_revision > 0),
    config_value jsonb NOT NULL,
    config_payload_id text NOT NULL CHECK (octet_length(config_payload_id) BETWEEN 1 AND 256),
    config_payload_digest text NOT NULL CHECK (config_payload_digest ~ '^sha256:[a-f0-9]{64}$'),
    validation_digest text NOT NULL CHECK (validation_digest ~ '^sha256:[a-f0-9]{64}$'),
    state text NOT NULL DEFAULT 'PLANNED' CHECK (state IN ('PLANNED','APPLIED')),
    applied_revision bigint,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    applied_at timestamptz,
    PRIMARY KEY (deployment_id,plan_id),
    UNIQUE (deployment_id,operation_id),
    CHECK ((target_generation_id IS NULL) = (target_graph_revision IS NULL)),
    CHECK ((state='PLANNED' AND applied_revision IS NULL AND applied_at IS NULL)
        OR (state='APPLIED' AND applied_revision IS NOT NULL AND applied_at IS NOT NULL))
);

CREATE TABLE module_configuration_revisions (
    deployment_id text NOT NULL REFERENCES forest_deployment.deployment_identity(deployment_id),
    revision bigint NOT NULL CHECK (revision > 0),
    previous_revision bigint NOT NULL CHECK (previous_revision = revision - 1),
    operation_id text NOT NULL CHECK (octet_length(operation_id) BETWEEN 1 AND 256),
    input_digest text NOT NULL CHECK (input_digest ~ '^sha256:[a-f0-9]{64}$'),
    plan_id text,
    config_value jsonb NOT NULL,
    config_payload_id text NOT NULL CHECK (octet_length(config_payload_id) BETWEEN 1 AND 256),
    config_payload_digest text NOT NULL CHECK (config_payload_digest ~ '^sha256:[a-f0-9]{64}$'),
    validation_digest text NOT NULL CHECK (validation_digest ~ '^sha256:[a-f0-9]{64}$'),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (deployment_id,revision),
    UNIQUE (deployment_id,operation_id),
    UNIQUE (deployment_id,plan_id),
    FOREIGN KEY (deployment_id,plan_id) REFERENCES module_configuration_plans(deployment_id,plan_id),
    CHECK ((revision=1 AND previous_revision=0 AND plan_id IS NULL)
        OR (revision>1 AND plan_id IS NOT NULL))
);

CREATE TABLE module_configuration_activations (
    deployment_id text NOT NULL REFERENCES forest_deployment.deployment_identity(deployment_id),
    operation_id text NOT NULL CHECK (octet_length(operation_id) BETWEEN 1 AND 256),
    input_digest text NOT NULL CHECK (input_digest ~ '^sha256:[a-f0-9]{64}$'),
    revision bigint NOT NULL CHECK (revision > 0),
    expected_effective_revision bigint NOT NULL CHECK (expected_effective_revision >= 0),
    generation_id text NOT NULL CHECK (octet_length(generation_id) BETWEEN 1 AND 256),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (deployment_id,operation_id),
    UNIQUE (deployment_id,generation_id),
    FOREIGN KEY (deployment_id,revision)
        REFERENCES module_configuration_revisions(deployment_id,revision)
);

CREATE FUNCTION forest_deployment_configuration_immutable() RETURNS trigger
LANGUAGE plpgsql SET search_path FROM CURRENT AS $$
BEGIN
    RAISE EXCEPTION 'Deployment module configuration fact is immutable' USING ERRCODE='55000';
END;
$$;

CREATE FUNCTION forest_deployment_configuration_plan_guard() RETURNS trigger
LANGUAGE plpgsql SET search_path FROM CURRENT AS $$
BEGIN
    IF ROW(OLD.deployment_id,OLD.plan_id,OLD.operation_id,OLD.input_digest,
           OLD.expected_desired_revision,OLD.target_generation_id,OLD.target_graph_revision,
           OLD.config_value,OLD.config_payload_id,
           OLD.config_payload_digest,OLD.validation_digest,OLD.created_at)
       IS DISTINCT FROM
       ROW(NEW.deployment_id,NEW.plan_id,NEW.operation_id,NEW.input_digest,
           NEW.expected_desired_revision,NEW.target_generation_id,NEW.target_graph_revision,
           NEW.config_value,NEW.config_payload_id,
           NEW.config_payload_digest,NEW.validation_digest,NEW.created_at)
       OR OLD.state='APPLIED'
       OR NOT (OLD.state='PLANNED' AND NEW.state='APPLIED') THEN
        RAISE EXCEPTION 'Deployment configuration plan transition is invalid'
            USING ERRCODE='55000';
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER module_configuration_revisions_immutable
    BEFORE UPDATE OR DELETE ON module_configuration_revisions
    FOR EACH ROW EXECUTE FUNCTION forest_deployment_configuration_immutable();
CREATE TRIGGER module_configuration_activations_immutable
    BEFORE UPDATE OR DELETE ON module_configuration_activations
    FOR EACH ROW EXECUTE FUNCTION forest_deployment_configuration_immutable();
CREATE TRIGGER module_configuration_plans_guard
    BEFORE UPDATE OR DELETE ON module_configuration_plans
    FOR EACH ROW EXECUTE FUNCTION forest_deployment_configuration_plan_guard();
