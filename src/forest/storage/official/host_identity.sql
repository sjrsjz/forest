CREATE SCHEMA forest_host;

CREATE TABLE forest_host.host_identity (
    singleton boolean PRIMARY KEY DEFAULT true CHECK (singleton),
    host_id text NOT NULL UNIQUE CHECK (host_id ~ '^host/[a-f0-9]{32}$'),
    baseline_sha256 text NOT NULL CHECK (baseline_sha256 ~ '^[a-f0-9]{64}$'),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp()
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
