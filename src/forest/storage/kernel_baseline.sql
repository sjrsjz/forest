CREATE SCHEMA forest_kernel;

CREATE TABLE forest_kernel.bot_identity (
    singleton boolean PRIMARY KEY DEFAULT true CHECK (singleton),
    bot_id text NOT NULL UNIQUE CHECK (bot_id <> '' AND length(bot_id) <= 128),
    kernel_baseline_sha256 text NOT NULL CHECK (
        kernel_baseline_sha256 ~ '^[0-9a-f]{64}$'
    ),
    initialized_at timestamptz NOT NULL DEFAULT clock_timestamp()
);

CREATE TABLE forest_kernel.schema_bundles (
    bot_id text NOT NULL REFERENCES forest_kernel.bot_identity(bot_id),
    schema_id text NOT NULL,
    component_id text NOT NULL,
    schema_version text NOT NULL,
    scope text NOT NULL CHECK (scope = 'bot'),
    namespace name NOT NULL,
    baseline_sha256 text NOT NULL CHECK (baseline_sha256 ~ '^[0-9a-f]{64}$'),
    package_digest text NOT NULL CHECK (package_digest ~ '^sha256:[0-9a-f]{64}$'),
    repository_contract text NOT NULL,
    execution_profile text NOT NULL CHECK (execution_profile IN ('declarative', 'official')),
    state text NOT NULL CHECK (state IN ('READY', 'DETACHED')),
    installed_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    detached_at timestamptz,
    PRIMARY KEY (bot_id, schema_id),
    UNIQUE (namespace),
    CHECK (
        (state = 'READY' AND detached_at IS NULL)
        OR (state = 'DETACHED' AND detached_at IS NOT NULL)
    )
);
