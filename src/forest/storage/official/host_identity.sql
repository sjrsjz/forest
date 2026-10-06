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

CREATE TABLE forest_host.runtime_graph_clock (
    host_id text PRIMARY KEY REFERENCES forest_host.host_identity(host_id),
    next_revision bigint NOT NULL DEFAULT 0 CHECK (next_revision >= 0),
    active_revision bigint NOT NULL DEFAULT 0 CHECK (active_revision >= 0),
    active_graph_id text,
    CHECK (active_revision <= next_revision)
);

CREATE TABLE forest_host.runtime_graphs (
    host_id text NOT NULL REFERENCES forest_host.host_identity(host_id),
    graph_id text NOT NULL,
    graph_revision bigint NOT NULL CHECK (graph_revision > 0),
    graph_digest text NOT NULL CHECK (graph_digest ~ '^sha256:[0-9a-f]{64}$'),
    activation_id text NOT NULL,
    source_graph_id text,
    source_revision bigint NOT NULL CHECK (source_revision >= 0),
    owner_epoch bigint NOT NULL CHECK (owner_epoch > 0),
    state text NOT NULL CHECK (state IN ('PREPARED','ACTIVE','RETIRED','FAILED')),
    prepared_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    published_at timestamptz,
    retired_at timestamptz,
    PRIMARY KEY (host_id,graph_id),
    UNIQUE (host_id,graph_revision),
    UNIQUE (host_id,activation_id),
    FOREIGN KEY (host_id,source_graph_id)
        REFERENCES forest_host.runtime_graphs(host_id,graph_id)
);

CREATE TABLE forest_host.runtime_generations (
    host_id text NOT NULL REFERENCES forest_host.host_identity(host_id),
    generation_id text NOT NULL,
    component_id text NOT NULL,
    binding_digest text NOT NULL CHECK (binding_digest ~ '^sha256:[0-9a-f]{64}$'),
    binding jsonb NOT NULL,
    owner_epoch bigint NOT NULL CHECK (owner_epoch > 0),
    state text NOT NULL CHECK (state IN ('PREPARED','ACTIVE','QUIESCING','STOPPED','FAILED')),
    failure_code text CHECK (
        failure_code IS NULL OR failure_code ~ '^[A-Z][A-Z0-9_]{0,127}$'
    ),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (host_id,generation_id)
);

CREATE TABLE forest_host.runtime_graph_members (
    host_id text NOT NULL,
    graph_id text NOT NULL,
    component_id text NOT NULL,
    generation_id text NOT NULL,
    data_admission boolean NOT NULL,
    control_admission boolean NOT NULL,
    route_epoch bigint NOT NULL DEFAULT 1 CHECK (route_epoch > 0),
    PRIMARY KEY (host_id,graph_id,component_id),
    UNIQUE (host_id,graph_id,generation_id),
    FOREIGN KEY (host_id,graph_id)
        REFERENCES forest_host.runtime_graphs(host_id,graph_id),
    FOREIGN KEY (host_id,generation_id)
        REFERENCES forest_host.runtime_generations(host_id,generation_id)
);

ALTER TABLE forest_host.runtime_graph_clock
    ADD FOREIGN KEY (host_id,active_graph_id)
    REFERENCES forest_host.runtime_graphs(host_id,graph_id);

CREATE TABLE forest_host.supervisor_lease (
    singleton boolean NOT NULL DEFAULT true UNIQUE CHECK (singleton),
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
