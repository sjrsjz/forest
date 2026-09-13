-- Deployment account lifecycle. Account rows remain stable identities because
-- audit, package and Bot directory facts retain their subject references.
CREATE TABLE web_account_plans (
    plan_id text PRIMARY KEY,
    subject_id text NOT NULL REFERENCES web_accounts(id),
    subject_revision bigint NOT NULL CHECK (subject_revision > 0),
    target_account_id text NOT NULL REFERENCES web_accounts(id),
    base_revision bigint NOT NULL CHECK (base_revision > 0),
    target_role text NOT NULL CHECK (target_role IN ('admin', 'viewer')),
    target_disabled boolean NOT NULL,
    password_hash_ciphertext text,
    password_secret_digest text,
    plan_digest text NOT NULL,
    expires_at timestamptz NOT NULL,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    CHECK ((password_hash_ciphertext IS NULL) = (password_secret_digest IS NULL))
);
CREATE INDEX web_account_plans_subject_idx ON web_account_plans(subject_id, created_at DESC);
CREATE TRIGGER web_account_plans_immutable BEFORE UPDATE OR DELETE ON web_account_plans
    FOR EACH ROW EXECUTE FUNCTION forest_reject_fact_mutation();

CREATE TABLE web_account_requests (
    request_id text PRIMARY KEY,
    subject_id text NOT NULL REFERENCES web_accounts(id),
    target_account_id text NOT NULL REFERENCES web_accounts(id),
    idempotency_key text NOT NULL,
    action text NOT NULL CHECK (action IN ('account.create', 'account.apply')),
    semantic_digest text NOT NULL,
    result jsonb NOT NULL,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (subject_id, idempotency_key)
);
CREATE TRIGGER web_account_requests_immutable BEFORE UPDATE OR DELETE ON web_account_requests
    FOR EACH ROW EXECUTE FUNCTION forest_reject_fact_mutation();

CREATE INDEX web_sessions_active_account_idx
    ON web_sessions(account_id, expires_at) WHERE revoked_at IS NULL;

-- Separate random material prevents password idempotency fingerprints from
-- reusing the Ed25519 signing seed. The encrypted key is never returned.
CREATE TABLE web_secret_digest_keys (
    issuer_id text PRIMARY KEY REFERENCES web_authority_keys(issuer_id),
    encrypted_key text NOT NULL,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp()
);
CREATE TRIGGER web_secret_digest_keys_immutable BEFORE UPDATE OR DELETE ON web_secret_digest_keys
    FOR EACH ROW EXECUTE FUNCTION forest_reject_fact_mutation();
