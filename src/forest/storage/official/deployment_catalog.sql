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

CREATE TABLE forest_deployment.web_account_operations (
    operation_id text PRIMARY KEY CHECK (octet_length(operation_id) BETWEEN 1 AND 256),
    operation_kind text NOT NULL CHECK (
        operation_kind IN ('BOOTSTRAP','CREATE','UPDATE','PASSWORD','REVOKE_SESSIONS')
    ),
    input_digest text NOT NULL CHECK (input_digest ~ '^sha256:[a-f0-9]{64}$'),
    credential_key_id text CHECK (octet_length(credential_key_id) BETWEEN 1 AND 256),
    subject_id text NOT NULL REFERENCES forest_deployment.web_accounts(subject_id),
    resulting_revision bigint NOT NULL CHECK (resulting_revision > 0),
    result jsonb NOT NULL CHECK (jsonb_typeof(result)='object'),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    CHECK (
        (operation_kind IN ('BOOTSTRAP','CREATE','PASSWORD')) =
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

CREATE INDEX web_login_throttles_expiry_idx
    ON forest_deployment.web_login_throttles(updated_at);

CREATE TABLE forest_deployment.storage_profiles (
    profile_ref text PRIMARY KEY CHECK (octet_length(profile_ref) BETWEEN 1 AND 512),
    bot_id text NOT NULL CHECK (octet_length(bot_id) BETWEEN 1 AND 128),
    storage_generation bigint NOT NULL CHECK (storage_generation > 0),
    dsn_secret_ref text NOT NULL CHECK (octet_length(dsn_secret_ref) BETWEEN 10 AND 512),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (profile_ref,bot_id,storage_generation)
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
        state IN ('QUEUED','RUNNING','RETRY_WAIT','SUCCEEDED','FAILED','CANCELLED')
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
    operation_kind text NOT NULL CHECK (operation_kind IN ('ENQUEUE','CANCEL')),
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
CREATE INDEX provisioning_claim_idx
    ON forest_deployment.provisioning_jobs(state,retry_not_before,created_at,job_id);

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

CREATE TRIGGER deployment_secret_version_immutable
    BEFORE UPDATE OR DELETE ON forest_deployment.secret_versions
    FOR EACH ROW EXECUTE FUNCTION forest_deployment.reject_identity_change();

CREATE TRIGGER deployment_storage_profile_immutable
    BEFORE UPDATE OR DELETE ON forest_deployment.storage_profiles
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
