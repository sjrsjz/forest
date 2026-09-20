CREATE SCHEMA forest_deployment;

CREATE TABLE forest_deployment.deployment_identity (
    singleton boolean PRIMARY KEY DEFAULT true CHECK (singleton),
    deployment_id text NOT NULL UNIQUE CHECK (octet_length(deployment_id) BETWEEN 1 AND 128),
    baseline_sha256 text NOT NULL CHECK (baseline_sha256 ~ '^[a-f0-9]{64}$'),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp()
);

CREATE TABLE forest_deployment.supervisor_lease (
    singleton boolean PRIMARY KEY DEFAULT true CHECK (singleton),
    host_id text,
    incarnation text,
    owner_epoch bigint NOT NULL DEFAULT 0 CHECK (owner_epoch >= 0),
    expires_at timestamptz NOT NULL DEFAULT '1970-01-01 00:00:00+00'
);

CREATE TABLE forest_deployment.bots (
    bot_id text PRIMARY KEY CHECK (octet_length(bot_id) BETWEEN 1 AND 128),
    display_name text NOT NULL CHECK (octet_length(display_name) BETWEEN 1 AND 256),
    supply_state text NOT NULL CHECK (supply_state IN ('READY','DELETING','DELETED')),
    storage_profile_ref text NOT NULL CHECK (octet_length(storage_profile_ref) BETWEEN 1 AND 512),
    storage_generation bigint NOT NULL CHECK (storage_generation > 0),
    revision bigint NOT NULL DEFAULT 1 CHECK (revision > 0),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp()
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

CREATE INDEX bots_supply_idx ON forest_deployment.bots(supply_state,created_at,bot_id);
CREATE INDEX observations_retry_idx
    ON forest_deployment.runtime_observations(observed_state,retry_not_before,bot_id);

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

CREATE TRIGGER deployment_secret_version_immutable
    BEFORE UPDATE OR DELETE ON forest_deployment.secret_versions
    FOR EACH ROW EXECUTE FUNCTION forest_deployment.reject_identity_change();

CREATE FUNCTION forest_deployment.guard_bot_change() RETURNS trigger LANGUAGE plpgsql AS '
BEGIN
    IF TG_OP = ''DELETE'' THEN
        RAISE EXCEPTION ''bot identity cannot be deleted'';
    END IF;
    IF ROW(NEW.bot_id,NEW.created_at) IS DISTINCT FROM ROW(OLD.bot_id,OLD.created_at) THEN
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
