CREATE SCHEMA forest_host;

CREATE TABLE forest_host.host_identity (
    singleton boolean PRIMARY KEY DEFAULT true CHECK (singleton),
    host_id text NOT NULL UNIQUE CHECK (host_id ~ '^host/[a-f0-9]{32}$'),
    baseline_sha256 text NOT NULL CHECK (baseline_sha256 ~ '^[a-f0-9]{64}$'),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp()
);

CREATE TABLE forest_host.schema_bundles (
    host_id text NOT NULL REFERENCES forest_host.host_identity(host_id),
    schema_id text NOT NULL,
    component_id text NOT NULL,
    schema_version text NOT NULL,
    scope text NOT NULL CHECK (scope = 'host'),
    namespace name NOT NULL UNIQUE,
    baseline_sha256 text NOT NULL CHECK (baseline_sha256 ~ '^[a-f0-9]{64}$'),
    package_digest text NOT NULL CHECK (package_digest ~ '^sha256:[a-f0-9]{64}$'),
    repository_contract text NOT NULL,
    execution_profile text NOT NULL CHECK (execution_profile IN ('declarative','official')),
    state text NOT NULL CHECK (state IN ('READY','DETACHED')),
    installed_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    detached_at timestamptz,
    PRIMARY KEY (host_id,schema_id),
    CHECK (
        (state='READY' AND detached_at IS NULL)
        OR (state='DETACHED' AND detached_at IS NOT NULL)
    )
);

CREATE TABLE forest_host.supervisor_lease (
    host_id text PRIMARY KEY REFERENCES forest_host.host_identity(host_id),
    incarnation text CHECK (incarnation ~ '^incarnation/[0-9a-f-]{36}$'),
    owner_epoch bigint NOT NULL DEFAULT 0 CHECK (owner_epoch >= 0),
    expires_at timestamptz NOT NULL DEFAULT '1970-01-01 00:00:00+00'
);

CREATE FUNCTION forest_host.reject_identity_change() RETURNS trigger
LANGUAGE plpgsql SET search_path = pg_catalog AS $$
BEGIN
    RAISE EXCEPTION 'Host identity is immutable' USING ERRCODE = '55000';
END;
$$;

CREATE TRIGGER host_identity_immutable
BEFORE UPDATE OR DELETE ON forest_host.host_identity
FOR EACH ROW EXECUTE FUNCTION forest_host.reject_identity_change();
