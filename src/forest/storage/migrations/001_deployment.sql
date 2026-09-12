CREATE TABLE web_accounts (
    id text PRIMARY KEY,
    username text NOT NULL UNIQUE,
    password_hash text NOT NULL,
    disabled boolean NOT NULL DEFAULT false,
    role text NOT NULL CHECK (role IN ('admin', 'viewer')),
    revision bigint NOT NULL DEFAULT 1,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp()
);

CREATE TABLE web_setup_state (
    singleton boolean PRIMARY KEY DEFAULT true CHECK (singleton),
    setup_id text NOT NULL UNIQUE,
    plan_digest text NOT NULL,
    account_id text NOT NULL REFERENCES web_accounts(id),
    completed_at timestamptz NOT NULL DEFAULT clock_timestamp()
);

CREATE TABLE web_sessions (
    token_hash text PRIMARY KEY,
    csrf_hash text NOT NULL,
    account_id text NOT NULL REFERENCES web_accounts(id),
    expires_at timestamptz NOT NULL,
    revoked_at timestamptz,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp()
);
CREATE INDEX web_sessions_account_idx ON web_sessions(account_id);

CREATE TABLE config_plans (
    plan_id text PRIMARY KEY,
    subject_id text NOT NULL REFERENCES web_accounts(id),
    resource_key text NOT NULL CHECK (resource_key = 'deployment'),
    base_revision bigint NOT NULL CHECK (base_revision >= 0),
    values jsonb NOT NULL,
    schema_digest text NOT NULL,
    plan_digest text NOT NULL,
    expires_at timestamptz NOT NULL,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp()
);

CREATE TABLE config_heads (
    resource_key text PRIMARY KEY CHECK (resource_key = 'deployment'),
    head_revision bigint NOT NULL DEFAULT 0 CHECK (head_revision >= 0),
    effective_revision bigint NOT NULL DEFAULT 0 CHECK (effective_revision >= 0),
    CHECK (effective_revision <= head_revision)
);

CREATE TABLE management_requests (
    request_id text PRIMARY KEY,
    subject_id text NOT NULL REFERENCES web_accounts(id),
    resource_key text NOT NULL CHECK (resource_key = 'deployment'),
    idempotency_key text NOT NULL,
    plan_id text NOT NULL REFERENCES config_plans(plan_id),
    plan_digest text NOT NULL,
    schema_digest text NOT NULL,
    expected_revision bigint NOT NULL,
    values jsonb NOT NULL,
    result jsonb NOT NULL,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (subject_id, resource_key, idempotency_key)
);

CREATE TABLE config_revisions (
    resource_key text NOT NULL REFERENCES config_heads(resource_key),
    revision bigint NOT NULL CHECK (revision > 0),
    previous_revision bigint NOT NULL CHECK (previous_revision = revision - 1),
    schema_digest text NOT NULL,
    values jsonb NOT NULL,
    plan_id text NOT NULL REFERENCES config_plans(plan_id),
    plan_digest text NOT NULL,
    request_id text NOT NULL UNIQUE REFERENCES management_requests(request_id),
    created_by text NOT NULL REFERENCES web_accounts(id),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (resource_key, revision)
);

CREATE TABLE audit_records (
    audit_id text PRIMARY KEY,
    subject_id text NOT NULL REFERENCES web_accounts(id),
    action text NOT NULL,
    resource_key text NOT NULL,
    request_id text,
    details jsonb NOT NULL,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp()
);

CREATE TABLE outbox_events (
    event_id text PRIMARY KEY,
    topic text NOT NULL,
    resource_key text NOT NULL,
    request_id text,
    payload jsonb NOT NULL,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    delivered_at timestamptz
);

-- Append-only facts cannot be accidentally updated by a repository operation.
CREATE FUNCTION forest_reject_fact_mutation() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    RAISE EXCEPTION 'Forest immutable fact cannot be changed' USING ERRCODE = '55000';
END;
$$;
CREATE TRIGGER config_revisions_immutable BEFORE UPDATE OR DELETE ON config_revisions
    FOR EACH ROW EXECUTE FUNCTION forest_reject_fact_mutation();
CREATE TRIGGER config_plans_immutable BEFORE UPDATE OR DELETE ON config_plans
    FOR EACH ROW EXECUTE FUNCTION forest_reject_fact_mutation();
CREATE TRIGGER management_requests_immutable BEFORE UPDATE OR DELETE ON management_requests
    FOR EACH ROW EXECUTE FUNCTION forest_reject_fact_mutation();
CREATE TRIGGER audit_records_immutable BEFORE UPDATE OR DELETE ON audit_records
    FOR EACH ROW EXECUTE FUNCTION forest_reject_fact_mutation();
