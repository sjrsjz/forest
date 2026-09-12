ALTER TABLE web_sessions ADD COLUMN revision bigint NOT NULL DEFAULT 1 CHECK (revision > 0);
CREATE FUNCTION forest_session_revision() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    NEW.revision := OLD.revision + 1;
    RETURN NEW;
END;
$$;
CREATE TRIGGER web_session_revision BEFORE UPDATE ON web_sessions
    FOR EACH ROW EXECUTE FUNCTION forest_session_revision();
CREATE TABLE web_authority_keys (
    issuer_id text PRIMARY KEY,
    key_revision bigint NOT NULL DEFAULT 1 CHECK (key_revision > 0),
    public_key text NOT NULL,
    encrypted_private_key text NOT NULL,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp()
);
CREATE TRIGGER web_authority_keys_immutable BEFORE UPDATE OR DELETE ON web_authority_keys
    FOR EACH ROW EXECUTE FUNCTION forest_reject_fact_mutation();
