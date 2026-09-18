CREATE TABLE media_captures (
    bot_id text NOT NULL REFERENCES forest_kernel.bot_identity(bot_id),
    capture_id text NOT NULL CHECK (octet_length(capture_id) BETWEEN 1 AND 256),
    operation_id text NOT NULL CHECK (octet_length(operation_id) BETWEEN 1 AND 256),
    input_digest text NOT NULL CHECK (input_digest ~ '^sha256:[a-f0-9]{64}$'),
    source_unit_id text NOT NULL CHECK (octet_length(source_unit_id) BETWEEN 1 AND 256),
    source_revision bigint NOT NULL CHECK (source_revision > 0),
    source_id text NOT NULL CHECK (octet_length(source_id) BETWEEN 1 AND 256),
    source_kind text NOT NULL CHECK (source_kind IN ('GROUP','DIRECT')),
    platform text NOT NULL CHECK (octet_length(platform) BETWEEN 1 AND 64),
    realm_id text NOT NULL CHECK (octet_length(realm_id) BETWEEN 1 AND 256),
    external_group_id text,
    trunk_id text,
    unit_kind text NOT NULL CHECK (octet_length(unit_kind) BETWEEN 1 AND 128),
    author_subject_ref text NOT NULL CHECK (octet_length(author_subject_ref) BETWEEN 1 AND 256),
    author_payload_id text NOT NULL CHECK (octet_length(author_payload_id) BETWEEN 1 AND 256),
    author_payload_digest text NOT NULL CHECK (author_payload_digest ~ '^sha256:[a-f0-9]{64}$'),
    author_owner_component_id text NOT NULL CHECK (octet_length(author_owner_component_id) BETWEEN 1 AND 128),
    author_owner_kind text NOT NULL CHECK (octet_length(author_owner_kind) BETWEEN 1 AND 128),
    author_owner_id text NOT NULL CHECK (octet_length(author_owner_id) BETWEEN 1 AND 512),
    binding_ref text NOT NULL CHECK (octet_length(binding_ref) BETWEEN 1 AND 256),
    conversation_ref text NOT NULL CHECK (octet_length(conversation_ref) BETWEEN 1 AND 256),
    event_ref text NOT NULL CHECK (octet_length(event_ref) BETWEEN 1 AND 256),
    ingress_seq bigint NOT NULL CHECK (ingress_seq > 0),
    observed_at timestamptz NOT NULL,
    occurred_at timestamptz NOT NULL,
    state text NOT NULL DEFAULT 'PENDING' CHECK (
        state IN ('PENDING','FETCHING','READY','PARTIAL','FAILED','CANCELLED')
    ),
    attempt_count integer NOT NULL DEFAULT 0 CHECK (attempt_count >= 0),
    next_attempt_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    lease_owner text,
    lease_deadline timestamptz,
    error_code text CHECK (error_code IS NULL OR error_code ~ '^[A-Z][A-Z0-9_]{0,127}$'),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    settled_at timestamptz,
    PRIMARY KEY (bot_id,capture_id),
    UNIQUE (bot_id,operation_id),
    UNIQUE (bot_id,source_unit_id),
    CHECK (
        (source_kind='GROUP' AND external_group_id IS NOT NULL AND trunk_id IS NOT NULL)
        OR (source_kind='DIRECT' AND external_group_id IS NULL AND trunk_id IS NULL)
    ),
    CHECK ((state='FETCHING') = (lease_owner IS NOT NULL AND lease_deadline IS NOT NULL)),
    CHECK ((state IN ('READY','PARTIAL','FAILED','CANCELLED')) = (settled_at IS NOT NULL))
);

CREATE TABLE media_capture_parts (
    bot_id text NOT NULL,
    capture_id text NOT NULL,
    part_id text NOT NULL CHECK (octet_length(part_id) BETWEEN 1 AND 256),
    part_index integer NOT NULL CHECK (part_index >= 0),
    part_kind text NOT NULL CHECK (part_kind IN ('TEXT','IMAGE')),
    state text NOT NULL CHECK (state IN ('PENDING','READY','FAILED')),
    media_ref text,
    locator jsonb,
    payload_id text,
    payload_digest text CHECK (payload_digest IS NULL OR payload_digest ~ '^sha256:[a-f0-9]{64}$'),
    payload_owner_component_id text,
    payload_owner_kind text,
    payload_owner_id text,
    media_type text,
    width integer CHECK (width IS NULL OR width > 0),
    height integer CHECK (height IS NULL OR height > 0),
    byte_length integer CHECK (byte_length IS NULL OR byte_length > 0),
    error_code text CHECK (error_code IS NULL OR error_code ~ '^[A-Z][A-Z0-9_]{0,127}$'),
    PRIMARY KEY (bot_id,capture_id,part_id),
    UNIQUE (bot_id,capture_id,part_index),
    UNIQUE (bot_id,media_ref),
    FOREIGN KEY (bot_id,capture_id) REFERENCES media_captures(bot_id,capture_id),
    CHECK (
        (part_kind='TEXT' AND state='READY' AND media_ref IS NULL AND locator IS NULL
            AND payload_id IS NOT NULL AND payload_digest IS NOT NULL
            AND media_type IS NOT NULL AND byte_length IS NOT NULL)
        OR (part_kind='IMAGE' AND media_ref IS NOT NULL AND locator IS NOT NULL)
    ),
    CHECK (
        (part_kind='IMAGE' AND state='READY' AND payload_id IS NOT NULL
            AND payload_digest IS NOT NULL AND media_type IS NOT NULL
            AND width IS NOT NULL AND height IS NOT NULL AND byte_length IS NOT NULL)
        OR (part_kind='IMAGE' AND state IN ('PENDING','FAILED') AND payload_id IS NULL
            AND payload_digest IS NULL AND media_type IS NULL AND width IS NULL
            AND height IS NULL AND byte_length IS NULL)
        OR part_kind='TEXT'
    )
);

CREATE TABLE media_assets (
    bot_id text NOT NULL REFERENCES forest_kernel.bot_identity(bot_id),
    media_ref text NOT NULL CHECK (octet_length(media_ref) BETWEEN 1 AND 256),
    media_revision bigint NOT NULL CHECK (media_revision > 0),
    capture_id text NOT NULL,
    part_id text NOT NULL,
    payload_id text NOT NULL CHECK (octet_length(payload_id) BETWEEN 1 AND 256),
    payload_digest text NOT NULL CHECK (payload_digest ~ '^sha256:[a-f0-9]{64}$'),
    media_type text NOT NULL CHECK (media_type IN ('image/png','image/jpeg','image/webp','image/gif')),
    width integer NOT NULL CHECK (width > 0),
    height integer NOT NULL CHECK (height > 0),
    byte_length integer NOT NULL CHECK (byte_length BETWEEN 1 AND 8388608),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (bot_id,media_ref,media_revision),
    UNIQUE (bot_id,capture_id,part_id),
    FOREIGN KEY (bot_id,capture_id,part_id)
        REFERENCES media_capture_parts(bot_id,capture_id,part_id)
);

CREATE INDEX media_capture_work_idx
    ON media_captures(bot_id,next_attempt_at,capture_id) WHERE state='PENDING';

CREATE FUNCTION forest_media_asset_immutable() RETURNS trigger
LANGUAGE plpgsql SET search_path FROM CURRENT AS $$
BEGIN
    RAISE EXCEPTION 'Forest media assets are immutable' USING ERRCODE='55000';
END;
$$;

CREATE TRIGGER media_assets_immutable BEFORE UPDATE OR DELETE ON media_assets
    FOR EACH ROW EXECUTE FUNCTION forest_media_asset_immutable();
