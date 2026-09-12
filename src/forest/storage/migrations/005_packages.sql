CREATE TABLE web_packages (
    package_digest text PRIMARY KEY,
    package_id text NOT NULL,
    version text NOT NULL,
    component_id text NOT NULL,
    registered_by text NOT NULL REFERENCES web_accounts(id),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    UNIQUE(package_id, version)
);
CREATE TRIGGER web_packages_immutable BEFORE UPDATE OR DELETE ON web_packages
    FOR EACH ROW EXECUTE FUNCTION forest_reject_fact_mutation();

CREATE TABLE web_package_requests (
    subject_id text NOT NULL REFERENCES web_accounts(id),
    idempotency_key text NOT NULL,
    input_digest text NOT NULL,
    package_digest text NOT NULL REFERENCES web_packages(package_digest),
    PRIMARY KEY(subject_id, idempotency_key)
);
CREATE TRIGGER web_package_requests_immutable BEFORE UPDATE OR DELETE ON web_package_requests
    FOR EACH ROW EXECUTE FUNCTION forest_reject_fact_mutation();
