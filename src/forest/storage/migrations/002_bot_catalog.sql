-- Deployment-owned directory and provisioning only. Bot business/configuration
-- facts remain in the separately initialized Bot database.
CREATE TABLE bot_catalog (
    bot_id text PRIMARY KEY,
    display_name text NOT NULL CHECK (length(display_name) BETWEEN 1 AND 120),
    mode text NOT NULL CHECK (mode IN ('managed', 'external')),
    stage text NOT NULL CHECK (stage IN ('PENDING', 'PROVISIONING', 'READY', 'FAILED')),
    revision bigint NOT NULL DEFAULT 1 CHECK (revision > 0),
    job_id text NOT NULL UNIQUE,
    error_code text,
    created_by text NOT NULL REFERENCES web_accounts(id),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp()
);

-- Immutable target, credentials and ownership proof precede every external DDL.
-- secret_ciphertext contains the DSN and (managed only) random SCRAM verifier.
CREATE TABLE bot_storage_plans (
    bot_id text PRIMARY KEY REFERENCES bot_catalog(bot_id),
    deployment_id text NOT NULL,
    plan_digest text NOT NULL,
    cluster_id text,
    database_name text NOT NULL,
    role_name text NOT NULL,
    secret_ciphertext text NOT NULL,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp()
);
CREATE TRIGGER bot_storage_plan_immutable BEFORE UPDATE OR DELETE ON bot_storage_plans
    FOR EACH ROW EXECUTE FUNCTION forest_reject_fact_mutation();

CREATE TABLE bot_provision_jobs (
    job_id text PRIMARY KEY,
    bot_id text NOT NULL UNIQUE REFERENCES bot_catalog(bot_id),
    subject_id text NOT NULL REFERENCES web_accounts(id),
    stage text NOT NULL CHECK (stage IN ('PENDING', 'PROVISIONING', 'READY', 'FAILED')),
    step text NOT NULL DEFAULT 'FROZEN',
    attempt bigint NOT NULL DEFAULT 0,
    owner_id text,
    owner_epoch bigint NOT NULL DEFAULT 0,
    lease_expires_at timestamptz,
    error_code text,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp()
);
CREATE INDEX bot_provision_resume ON bot_provision_jobs(stage, lease_expires_at);

-- This is a deployment provisioning job, not another execution ledger for Bot
-- lifecycle operations. Events retain permits/unknown outcomes across crashes.
CREATE TABLE bot_provision_events (
    event_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    job_id text NOT NULL REFERENCES bot_provision_jobs(job_id),
    owner_epoch bigint NOT NULL,
    step text NOT NULL,
    details jsonb NOT NULL DEFAULT '{}',
    created_at timestamptz NOT NULL DEFAULT clock_timestamp()
);
CREATE TRIGGER bot_provision_event_immutable BEFORE UPDATE OR DELETE ON bot_provision_events
    FOR EACH ROW EXECUTE FUNCTION forest_reject_fact_mutation();

CREATE TABLE bot_catalog_requests (
    request_id text PRIMARY KEY,
    subject_id text NOT NULL REFERENCES web_accounts(id),
    idempotency_key text NOT NULL,
    semantic_digest text NOT NULL,
    bot_id text NOT NULL REFERENCES bot_catalog(bot_id),
    result jsonb NOT NULL,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (subject_id, idempotency_key)
);
CREATE TRIGGER bot_catalog_request_immutable BEFORE UPDATE OR DELETE ON bot_catalog_requests
    FOR EACH ROW EXECUTE FUNCTION forest_reject_fact_mutation();
