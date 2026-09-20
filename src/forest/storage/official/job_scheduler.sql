CREATE TABLE jobs (
    bot_id text NOT NULL REFERENCES forest_kernel.bot_identity(bot_id),
    job_id text NOT NULL CHECK (octet_length(job_id) BETWEEN 1 AND 256),
    operation_id text NOT NULL CHECK (octet_length(operation_id) BETWEEN 1 AND 256),
    input_digest text NOT NULL CHECK (input_digest ~ '^sha256:[a-f0-9]{64}$'),
    job_type text NOT NULL CHECK (job_type ~ '^[a-z0-9][a-z0-9._-]{0,127}$'),
    job_version integer NOT NULL CHECK (job_version > 0),
    input_schema_digest text NOT NULL CHECK (
        input_schema_digest ~ '^sha256:[a-f0-9]{64}$'
    ),
    input_payload_id text NOT NULL CHECK (octet_length(input_payload_id) BETWEEN 1 AND 256),
    input_payload_digest text NOT NULL CHECK (
        input_payload_digest ~ '^sha256:[a-f0-9]{64}$'
    ),
    requested_by text NOT NULL CHECK (octet_length(requested_by) BETWEEN 1 AND 256),
    priority smallint NOT NULL DEFAULT 4 CHECK (priority BETWEEN 0 AND 4),
    max_attempts integer NOT NULL CHECK (max_attempts BETWEEN 1 AND 32),
    timeout_seconds integer NOT NULL CHECK (timeout_seconds BETWEEN 1 AND 86400),
    max_concurrency integer NOT NULL CHECK (max_concurrency BETWEEN 1 AND 128),
    state text NOT NULL DEFAULT 'QUEUED' CHECK (
        state IN ('QUEUED','RUNNING','SUCCEEDED','FAILED','CANCELLED','UNKNOWN')
    ),
    revision bigint NOT NULL DEFAULT 1 CHECK (revision > 0),
    attempt_count integer NOT NULL DEFAULT 0 CHECK (
        attempt_count BETWEEN 0 AND max_attempts
    ),
    claim_token uuid,
    worker_component_id text,
    worker_generation_id text,
    target_component_id text,
    target_generation_id text,
    target_service_id text,
    target_service_version integer,
    lease_until timestamptz,
    cancel_requested_at timestamptz,
    cancel_requested_by text,
    cancel_reason_code text,
    result_payload_id text,
    result_payload_digest text CHECK (
        result_payload_digest IS NULL OR result_payload_digest ~ '^sha256:[a-f0-9]{64}$'
    ),
    error_code text CHECK (
        error_code IS NULL OR error_code ~ '^[A-Z][A-Z0-9_]{0,127}$'
    ),
    available_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    started_at timestamptz,
    completed_at timestamptz,
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (bot_id,job_id),
    UNIQUE (bot_id,operation_id),
    CHECK ((result_payload_id IS NULL) = (result_payload_digest IS NULL)),
    CHECK ((cancel_requested_at IS NULL) = (cancel_requested_by IS NULL)),
    CHECK ((cancel_requested_at IS NULL) = (cancel_reason_code IS NULL)),
    CHECK (cancel_requested_by IS NULL OR octet_length(cancel_requested_by) BETWEEN 1 AND 256),
    CHECK (cancel_reason_code IS NULL OR cancel_reason_code ~ '^[A-Z][A-Z0-9_]{0,127}$'),
    CHECK (
        (state='RUNNING' AND claim_token IS NOT NULL AND worker_component_id IS NOT NULL
            AND worker_generation_id IS NOT NULL AND target_component_id IS NOT NULL
            AND target_generation_id IS NOT NULL AND target_service_id IS NOT NULL
            AND target_service_version IS NOT NULL AND lease_until IS NOT NULL
            AND started_at IS NOT NULL AND completed_at IS NULL)
        OR (state<>'RUNNING' AND claim_token IS NULL AND worker_component_id IS NULL
            AND worker_generation_id IS NULL AND target_component_id IS NULL
            AND target_generation_id IS NULL AND target_service_id IS NULL
            AND target_service_version IS NULL AND lease_until IS NULL)
    ),
    CHECK (
        (state='SUCCEEDED' AND result_payload_id IS NOT NULL AND error_code IS NULL
            AND completed_at IS NOT NULL)
        OR (state IN ('FAILED','UNKNOWN') AND result_payload_id IS NULL
            AND error_code IS NOT NULL AND completed_at IS NOT NULL)
        OR (state='CANCELLED' AND result_payload_id IS NULL AND error_code IS NULL
            AND completed_at IS NOT NULL)
        OR (state IN ('QUEUED','RUNNING') AND result_payload_id IS NULL
            AND error_code IS NULL AND completed_at IS NULL)
    )
);

CREATE INDEX jobs_claim_idx
    ON jobs(bot_id,priority,available_at,created_at,job_id)
    WHERE state IN ('QUEUED','RUNNING');

CREATE INDEX jobs_recent_idx
    ON jobs(bot_id,created_at DESC,job_id DESC);

CREATE INDEX jobs_type_state_idx
    ON jobs(bot_id,job_type,state,created_at DESC,job_id DESC);

CREATE TABLE cancellation_requests (
    bot_id text NOT NULL REFERENCES forest_kernel.bot_identity(bot_id),
    operation_id text NOT NULL CHECK (octet_length(operation_id) BETWEEN 1 AND 256),
    input_digest text NOT NULL CHECK (input_digest ~ '^sha256:[a-f0-9]{64}$'),
    job_id text NOT NULL,
    expected_revision bigint NOT NULL CHECK (expected_revision > 0),
    requested_by text NOT NULL CHECK (octet_length(requested_by) BETWEEN 1 AND 256),
    reason_code text NOT NULL CHECK (reason_code ~ '^[A-Z][A-Z0-9_]{0,127}$'),
    resulting_state text NOT NULL CHECK (
        resulting_state IN ('RUNNING','CANCELLED','SUCCEEDED','FAILED','UNKNOWN')
    ),
    resulting_revision bigint NOT NULL CHECK (resulting_revision > 0),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (bot_id,operation_id),
    UNIQUE (bot_id,job_id),
    FOREIGN KEY (bot_id,job_id) REFERENCES jobs(bot_id,job_id)
);

CREATE FUNCTION guard_job_update() RETURNS trigger LANGUAGE plpgsql AS '
BEGIN
    IF TG_OP = ''DELETE'' THEN
        RAISE EXCEPTION ''jobs are immutable ledger facts'';
    END IF;
    IF ROW(NEW.bot_id,NEW.job_id,NEW.operation_id,NEW.input_digest,NEW.job_type,
        NEW.job_version,NEW.input_schema_digest,NEW.input_payload_id,
        NEW.input_payload_digest,NEW.requested_by,NEW.priority,NEW.max_attempts,
        NEW.timeout_seconds,NEW.max_concurrency,NEW.created_at)
       IS DISTINCT FROM
       ROW(OLD.bot_id,OLD.job_id,OLD.operation_id,OLD.input_digest,OLD.job_type,
        OLD.job_version,OLD.input_schema_digest,OLD.input_payload_id,
        OLD.input_payload_digest,OLD.requested_by,OLD.priority,OLD.max_attempts,
        OLD.timeout_seconds,OLD.max_concurrency,OLD.created_at) THEN
        RAISE EXCEPTION ''job identity is immutable'';
    END IF;
    IF NEW.revision = OLD.revision THEN
        IF OLD.state <> ''RUNNING'' OR NEW.state <> ''RUNNING''
           OR NEW.lease_until <= OLD.lease_until
           OR NEW.updated_at < OLD.updated_at
           OR ROW(NEW.attempt_count,NEW.claim_token,NEW.worker_component_id,
                NEW.worker_generation_id,NEW.target_component_id,
                NEW.target_generation_id,NEW.target_service_id,
                NEW.target_service_version,NEW.cancel_requested_at,
                NEW.cancel_requested_by,NEW.cancel_reason_code,
                NEW.result_payload_id,NEW.result_payload_digest,NEW.error_code,
                NEW.available_at,NEW.started_at,NEW.completed_at)
              IS DISTINCT FROM
              ROW(OLD.attempt_count,OLD.claim_token,OLD.worker_component_id,
                OLD.worker_generation_id,OLD.target_component_id,
                OLD.target_generation_id,OLD.target_service_id,
                OLD.target_service_version,OLD.cancel_requested_at,
                OLD.cancel_requested_by,OLD.cancel_reason_code,
                OLD.result_payload_id,OLD.result_payload_digest,OLD.error_code,
                OLD.available_at,OLD.started_at,OLD.completed_at) THEN
            RAISE EXCEPTION ''same-revision update is not a lease renewal'';
        END IF;
        RETURN NEW;
    ELSIF NEW.revision <> OLD.revision + 1 THEN
        RAISE EXCEPTION ''job revision must advance once'';
    END IF;
    IF OLD.state IN (''SUCCEEDED'',''FAILED'',''CANCELLED'',''UNKNOWN'') THEN
        RAISE EXCEPTION ''terminal job is immutable'';
    END IF;
    IF NOT (
        (OLD.state=''QUEUED'' AND NEW.state IN (''RUNNING'',''CANCELLED'',''FAILED''))
        OR (OLD.state=''RUNNING'' AND NEW.state IN
            (''RUNNING'',''QUEUED'',''SUCCEEDED'',''FAILED'',''CANCELLED'',''UNKNOWN''))
    ) THEN
        RAISE EXCEPTION ''illegal job transition'';
    END IF;
    RETURN NEW;
END;
';

CREATE TRIGGER job_update_guard
    BEFORE UPDATE OR DELETE ON jobs
    FOR EACH ROW EXECUTE FUNCTION guard_job_update();

CREATE FUNCTION reject_cancellation_change() RETURNS trigger LANGUAGE plpgsql AS '
BEGIN
    RAISE EXCEPTION ''cancellation request is immutable'';
END;
';

CREATE TRIGGER cancellation_change_guard
    BEFORE UPDATE OR DELETE ON cancellation_requests
    FOR EACH ROW EXECUTE FUNCTION reject_cancellation_change();
