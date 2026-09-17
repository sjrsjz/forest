CREATE TABLE module_configuration_heads (
    bot_id text PRIMARY KEY REFERENCES forest_kernel.bot_identity(bot_id),
    desired_revision bigint NOT NULL DEFAULT 0 CHECK (desired_revision >= 0),
    effective_revision bigint NOT NULL DEFAULT 0 CHECK (
        effective_revision BETWEEN 0 AND desired_revision
    ),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    CHECK (
        (desired_revision=0 AND effective_revision=0)
        OR (desired_revision>0 AND effective_revision>0)
    )
);

CREATE TABLE module_configuration_plans (
    bot_id text NOT NULL REFERENCES forest_kernel.bot_identity(bot_id),
    plan_id text NOT NULL CHECK (octet_length(plan_id) BETWEEN 1 AND 256),
    operation_id text NOT NULL CHECK (octet_length(operation_id) BETWEEN 1 AND 256),
    input_digest text NOT NULL CHECK (input_digest ~ '^sha256:[a-f0-9]{64}$'),
    expected_desired_revision bigint NOT NULL CHECK (expected_desired_revision >= 0),
    config_payload_id text NOT NULL CHECK (octet_length(config_payload_id) BETWEEN 1 AND 256),
    config_payload_digest text NOT NULL CHECK (
        config_payload_digest ~ '^sha256:[a-f0-9]{64}$'
    ),
    validation_digest text NOT NULL CHECK (validation_digest ~ '^sha256:[a-f0-9]{64}$'),
    state text NOT NULL DEFAULT 'PLANNED' CHECK (state IN ('PLANNED','APPLIED')),
    applied_revision bigint,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    applied_at timestamptz,
    PRIMARY KEY (bot_id,plan_id),
    UNIQUE (bot_id,operation_id),
    CHECK (
        (state='PLANNED' AND applied_revision IS NULL AND applied_at IS NULL)
        OR (state='APPLIED' AND applied_revision IS NOT NULL AND applied_at IS NOT NULL)
    )
);

CREATE TABLE module_configuration_revisions (
    bot_id text NOT NULL REFERENCES forest_kernel.bot_identity(bot_id),
    revision bigint NOT NULL CHECK (revision > 0),
    previous_revision bigint NOT NULL CHECK (previous_revision = revision - 1),
    operation_id text NOT NULL CHECK (octet_length(operation_id) BETWEEN 1 AND 256),
    input_digest text NOT NULL CHECK (input_digest ~ '^sha256:[a-f0-9]{64}$'),
    plan_id text,
    config_payload_id text NOT NULL CHECK (octet_length(config_payload_id) BETWEEN 1 AND 256),
    config_payload_digest text NOT NULL CHECK (
        config_payload_digest ~ '^sha256:[a-f0-9]{64}$'
    ),
    validation_digest text NOT NULL CHECK (validation_digest ~ '^sha256:[a-f0-9]{64}$'),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (bot_id,revision),
    UNIQUE (bot_id,operation_id),
    UNIQUE (bot_id,plan_id),
    FOREIGN KEY (bot_id,plan_id) REFERENCES module_configuration_plans(bot_id,plan_id),
    CHECK ((revision=1 AND previous_revision=0) OR plan_id IS NOT NULL)
);

CREATE TABLE module_configuration_activations (
    bot_id text NOT NULL REFERENCES forest_kernel.bot_identity(bot_id),
    operation_id text NOT NULL CHECK (octet_length(operation_id) BETWEEN 1 AND 256),
    input_digest text NOT NULL CHECK (input_digest ~ '^sha256:[a-f0-9]{64}$'),
    revision bigint NOT NULL CHECK (revision > 0),
    expected_effective_revision bigint NOT NULL CHECK (expected_effective_revision >= 0),
    generation_id text NOT NULL CHECK (octet_length(generation_id) BETWEEN 1 AND 256),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (bot_id,operation_id),
    UNIQUE (bot_id,generation_id),
    FOREIGN KEY (bot_id,revision)
        REFERENCES module_configuration_revisions(bot_id,revision)
);

CREATE FUNCTION forest_module_configuration_revision_immutable() RETURNS trigger
LANGUAGE plpgsql SET search_path FROM CURRENT AS $$
BEGIN
    RAISE EXCEPTION 'Forest module configuration revision is immutable' USING ERRCODE='55000';
END;
$$;

CREATE FUNCTION forest_module_configuration_plan_guard() RETURNS trigger
LANGUAGE plpgsql SET search_path FROM CURRENT AS $$
BEGIN
    IF ROW(OLD.bot_id,OLD.plan_id,OLD.operation_id,OLD.input_digest,
           OLD.expected_desired_revision,OLD.config_payload_id,OLD.config_payload_digest,
           OLD.validation_digest,OLD.created_at)
       IS DISTINCT FROM
       ROW(NEW.bot_id,NEW.plan_id,NEW.operation_id,NEW.input_digest,
           NEW.expected_desired_revision,NEW.config_payload_id,NEW.config_payload_digest,
           NEW.validation_digest,NEW.created_at)
       OR OLD.state='APPLIED'
       OR NOT (OLD.state='PLANNED' AND NEW.state='APPLIED') THEN
        RAISE EXCEPTION 'Forest module configuration plan transition is invalid'
            USING ERRCODE='55000';
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER module_configuration_revisions_immutable
    BEFORE UPDATE OR DELETE ON module_configuration_revisions
    FOR EACH ROW EXECUTE FUNCTION forest_module_configuration_revision_immutable();
CREATE TRIGGER module_configuration_activations_immutable
    BEFORE UPDATE OR DELETE ON module_configuration_activations
    FOR EACH ROW EXECUTE FUNCTION forest_module_configuration_revision_immutable();
CREATE TRIGGER module_configuration_plans_guard
    BEFORE UPDATE OR DELETE ON module_configuration_plans
    FOR EACH ROW EXECUTE FUNCTION forest_module_configuration_plan_guard();

INSERT INTO module_configuration_heads(bot_id)
SELECT bot_id FROM forest_kernel.bot_identity;
