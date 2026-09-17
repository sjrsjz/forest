CREATE TABLE deletion_clock (
    bot_id text PRIMARY KEY REFERENCES forest_kernel.bot_identity(bot_id),
    deletion_epoch bigint NOT NULL DEFAULT 0 CHECK (deletion_epoch >= 0)
);

CREATE TABLE source_manifests (
    bot_id text NOT NULL REFERENCES forest_kernel.bot_identity(bot_id),
    manifest_id text NOT NULL CHECK (manifest_id ~ '^manifest:[a-f0-9]{64}$'),
    manifest_digest text NOT NULL CHECK (manifest_digest ~ '^sha256:[a-f0-9]{64}$'),
    item_count integer NOT NULL CHECK (item_count BETWEEN 0 AND 4096),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (bot_id, manifest_id),
    UNIQUE (bot_id, manifest_digest)
);

CREATE TABLE source_manifest_items (
    bot_id text NOT NULL,
    manifest_id text NOT NULL,
    item_index integer NOT NULL CHECK (item_index BETWEEN 0 AND 4095),
    source_component_id text NOT NULL
        CHECK (octet_length(source_component_id) BETWEEN 1 AND 128),
    source_kind text NOT NULL CHECK (octet_length(source_kind) BETWEEN 1 AND 64),
    source_id text NOT NULL CHECK (octet_length(source_id) BETWEEN 1 AND 256),
    source_revision bigint NOT NULL CHECK (source_revision > 0),
    part_id text NOT NULL CHECK (octet_length(part_id) BETWEEN 1 AND 256),
    source_trunk_id text,
    observation_conversation_id text,
    observation_binding_id text,
    author_subject_id text,
    occurred_at timestamptz,
    propagation_class text NOT NULL
        CHECK (octet_length(propagation_class) BETWEEN 1 AND 64),
    PRIMARY KEY (bot_id, manifest_id, item_index),
    UNIQUE (
        bot_id, manifest_id, source_component_id, source_kind,
        source_id, source_revision, part_id
    ),
    FOREIGN KEY (bot_id, manifest_id)
        REFERENCES source_manifests(bot_id, manifest_id),
    CHECK (source_trunk_id IS NULL OR octet_length(source_trunk_id) BETWEEN 1 AND 256),
    CHECK (
        observation_conversation_id IS NULL
        OR octet_length(observation_conversation_id) BETWEEN 1 AND 256
    ),
    CHECK (
        observation_binding_id IS NULL
        OR octet_length(observation_binding_id) BETWEEN 1 AND 256
    ),
    CHECK (author_subject_id IS NULL OR octet_length(author_subject_id) BETWEEN 1 AND 256)
);

CREATE TABLE forget_requests (
    bot_id text NOT NULL REFERENCES forest_kernel.bot_identity(bot_id),
    operation_id text NOT NULL CHECK (octet_length(operation_id) BETWEEN 1 AND 256),
    input_digest text NOT NULL CHECK (input_digest ~ '^sha256:[a-f0-9]{64}$'),
    selection_digest text NOT NULL CHECK (selection_digest ~ '^sha256:[a-f0-9]{64}$'),
    selected_count integer NOT NULL CHECK (selected_count BETWEEN 0 AND 100000),
    deletion_epoch bigint NOT NULL CHECK (deletion_epoch > 0),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (bot_id, operation_id),
    UNIQUE (bot_id, deletion_epoch)
);

CREATE TABLE forget_source_targets (
    bot_id text NOT NULL,
    operation_id text NOT NULL,
    source_component_id text NOT NULL,
    source_kind text NOT NULL,
    source_id text NOT NULL,
    source_revision bigint NOT NULL CHECK (source_revision > 0),
    part_id text NOT NULL,
    PRIMARY KEY (
        bot_id, operation_id, source_component_id, source_kind,
        source_id, source_revision, part_id
    ),
    FOREIGN KEY (bot_id, operation_id)
        REFERENCES forget_requests(bot_id, operation_id),
    CHECK (octet_length(source_component_id) BETWEEN 1 AND 128),
    CHECK (octet_length(source_kind) BETWEEN 1 AND 64),
    CHECK (octet_length(source_id) BETWEEN 1 AND 256),
    CHECK (octet_length(part_id) BETWEEN 1 AND 256)
);

CREATE TABLE payload_headers (
    bot_id text NOT NULL REFERENCES forest_kernel.bot_identity(bot_id),
    payload_id text NOT NULL CHECK (payload_id ~ '^payload:[a-f0-9]{64}$'),
    owner_component_id text NOT NULL
        CHECK (octet_length(owner_component_id) BETWEEN 1 AND 128),
    owner_kind text NOT NULL CHECK (octet_length(owner_kind) BETWEEN 1 AND 128),
    owner_id text NOT NULL CHECK (octet_length(owner_id) BETWEEN 1 AND 512),
    media_type text NOT NULL CHECK (octet_length(media_type) BETWEEN 1 AND 128),
    byte_length bigint NOT NULL CHECK (byte_length BETWEEN 0 AND 33554432),
    content_digest text NOT NULL CHECK (content_digest ~ '^sha256:[a-f0-9]{64}$'),
    input_digest text NOT NULL CHECK (input_digest ~ '^sha256:[a-f0-9]{64}$'),
    source_manifest_id text NOT NULL,
    source_manifest_digest text NOT NULL CHECK (
        source_manifest_digest ~ '^sha256:[a-f0-9]{64}$'
    ),
    parent_count integer NOT NULL CHECK (parent_count BETWEEN 0 AND 256),
    accepted_deletion_epoch bigint NOT NULL CHECK (accepted_deletion_epoch >= 0),
    state text NOT NULL DEFAULT 'ACTIVE' CHECK (state IN ('ACTIVE', 'REVOKED', 'SCRUBBED')),
    revoke_operation_id text,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    revoked_at timestamptz,
    scrubbed_at timestamptz,
    PRIMARY KEY (bot_id, payload_id),
    UNIQUE (bot_id, owner_component_id, owner_kind, owner_id),
    FOREIGN KEY (bot_id, source_manifest_id)
        REFERENCES source_manifests(bot_id, manifest_id),
    FOREIGN KEY (bot_id, revoke_operation_id)
        REFERENCES forget_requests(bot_id, operation_id),
    CHECK (
        (state = 'ACTIVE' AND revoke_operation_id IS NULL
            AND revoked_at IS NULL AND scrubbed_at IS NULL)
        OR (state = 'REVOKED' AND revoke_operation_id IS NOT NULL
            AND revoked_at IS NOT NULL AND scrubbed_at IS NULL)
        OR (state = 'SCRUBBED' AND revoke_operation_id IS NOT NULL
            AND revoked_at IS NOT NULL AND scrubbed_at IS NOT NULL)
    )
);

CREATE TABLE forget_payload_targets (
    bot_id text NOT NULL,
    operation_id text NOT NULL,
    payload_id text NOT NULL,
    PRIMARY KEY (bot_id, operation_id, payload_id),
    FOREIGN KEY (bot_id, operation_id)
        REFERENCES forget_requests(bot_id, operation_id),
    FOREIGN KEY (bot_id, payload_id)
        REFERENCES payload_headers(bot_id, payload_id)
);

CREATE TABLE payload_derivations (
    bot_id text NOT NULL,
    payload_id text NOT NULL,
    parent_index integer NOT NULL CHECK (parent_index BETWEEN 0 AND 255),
    parent_payload_id text NOT NULL,
    PRIMARY KEY (bot_id, payload_id, parent_index),
    UNIQUE (bot_id, payload_id, parent_payload_id),
    FOREIGN KEY (bot_id, payload_id)
        REFERENCES payload_headers(bot_id, payload_id),
    FOREIGN KEY (bot_id, parent_payload_id)
        REFERENCES payload_headers(bot_id, payload_id),
    CHECK (payload_id <> parent_payload_id)
);

CREATE TABLE payload_bodies (
    bot_id text NOT NULL,
    payload_id text NOT NULL,
    content bytea NOT NULL,
    PRIMARY KEY (bot_id, payload_id),
    FOREIGN KEY (bot_id, payload_id)
        REFERENCES payload_headers(bot_id, payload_id)
);

CREATE INDEX manifest_source_idx ON source_manifest_items(
    bot_id, source_component_id, source_kind, source_id, source_revision, part_id, manifest_id
);
CREATE INDEX payload_manifest_idx ON payload_headers(bot_id, source_manifest_id, payload_id);
CREATE INDEX payload_scrub_pending_idx ON payload_headers(bot_id, payload_id)
    WHERE state = 'REVOKED';
CREATE INDEX payload_parent_idx ON payload_derivations(bot_id, parent_payload_id, payload_id);

CREATE FUNCTION reject_immutable_fact() RETURNS trigger LANGUAGE plpgsql
SET search_path FROM CURRENT AS $$
BEGIN
    RAISE EXCEPTION 'Forest payload fact is immutable' USING ERRCODE = '55000';
END;
$$;

CREATE TRIGGER manifests_immutable BEFORE UPDATE OR DELETE ON source_manifests
    FOR EACH ROW EXECUTE FUNCTION reject_immutable_fact();
CREATE TRIGGER manifest_items_immutable BEFORE UPDATE OR DELETE ON source_manifest_items
    FOR EACH ROW EXECUTE FUNCTION reject_immutable_fact();
CREATE TRIGGER forget_requests_immutable BEFORE UPDATE OR DELETE ON forget_requests
    FOR EACH ROW EXECUTE FUNCTION reject_immutable_fact();
CREATE TRIGGER forget_source_targets_immutable BEFORE UPDATE OR DELETE ON forget_source_targets
    FOR EACH ROW EXECUTE FUNCTION reject_immutable_fact();
CREATE TRIGGER forget_payload_targets_immutable BEFORE UPDATE OR DELETE ON forget_payload_targets
    FOR EACH ROW EXECUTE FUNCTION reject_immutable_fact();
CREATE TRIGGER derivations_immutable BEFORE UPDATE OR DELETE ON payload_derivations
    FOR EACH ROW EXECUTE FUNCTION reject_immutable_fact();

CREATE FUNCTION deletion_clock_guard() RETURNS trigger LANGUAGE plpgsql
SET search_path FROM CURRENT AS $$
BEGIN
    IF TG_OP = 'DELETE' OR NEW.bot_id <> OLD.bot_id
       OR NEW.deletion_epoch <> OLD.deletion_epoch + 1 THEN
        RAISE EXCEPTION 'Payload deletion clock must advance exactly once' USING ERRCODE = '55000';
    END IF;
    RETURN NEW;
END;
$$;
CREATE TRIGGER deletion_clock_monotonic BEFORE UPDATE OR DELETE ON deletion_clock
    FOR EACH ROW EXECUTE FUNCTION deletion_clock_guard();

CREATE FUNCTION manifest_item_guard() RETURNS trigger LANGUAGE plpgsql
SET search_path FROM CURRENT AS $$
DECLARE expected_count integer;
BEGIN
    SELECT item_count INTO expected_count FROM source_manifests
        WHERE bot_id = NEW.bot_id AND manifest_id = NEW.manifest_id;
    IF expected_count IS NULL OR NEW.item_index >= expected_count THEN
        RAISE EXCEPTION 'Payload source item lies outside its manifest' USING ERRCODE = '55000';
    END IF;
    RETURN NEW;
END;
$$;
CREATE TRIGGER manifest_item_admission BEFORE INSERT ON source_manifest_items
    FOR EACH ROW EXECUTE FUNCTION manifest_item_guard();

CREATE FUNCTION payload_header_guard() RETURNS trigger LANGUAGE plpgsql
SET search_path FROM CURRENT AS $$
BEGIN
    IF TG_OP = 'DELETE' OR (
        to_jsonb(NEW) - ARRAY['state','revoke_operation_id','revoked_at','scrubbed_at']
    ) IS DISTINCT FROM (
        to_jsonb(OLD) - ARRAY['state','revoke_operation_id','revoked_at','scrubbed_at']
    ) THEN
        RAISE EXCEPTION 'Payload identity is immutable' USING ERRCODE = '55000';
    END IF;
    IF NEW IS NOT DISTINCT FROM OLD THEN
        RETURN NEW;
    END IF;
    IF OLD.state = 'ACTIVE' AND NEW.state = 'REVOKED'
       AND NEW.revoke_operation_id IS NOT NULL
       AND NEW.revoked_at IS NOT NULL
       AND NEW.scrubbed_at IS NULL
       AND (
           EXISTS (
               SELECT 1 FROM forget_payload_targets t
               WHERE t.bot_id = NEW.bot_id
                 AND t.operation_id = NEW.revoke_operation_id
                 AND t.payload_id = NEW.payload_id
           )
           OR EXISTS (
               SELECT 1
               FROM source_manifest_items i
               JOIN forget_source_targets t
                 ON t.bot_id = i.bot_id
                AND t.operation_id = NEW.revoke_operation_id
                AND t.source_component_id = i.source_component_id
                AND t.source_kind = i.source_kind
                AND t.source_id = i.source_id
                AND t.source_revision = i.source_revision
                AND (t.part_id = '*' OR i.part_id = '*' OR t.part_id = i.part_id)
               WHERE i.bot_id = NEW.bot_id
                 AND i.manifest_id = NEW.source_manifest_id
           )
       ) THEN
        RETURN NEW;
    END IF;
    IF OLD.state = 'REVOKED' AND NEW.state = 'SCRUBBED'
       AND NEW.revoke_operation_id = OLD.revoke_operation_id
       AND NEW.revoked_at = OLD.revoked_at
       AND NEW.scrubbed_at IS NOT NULL
       AND NOT EXISTS (
           SELECT 1 FROM payload_bodies b
           WHERE b.bot_id = NEW.bot_id AND b.payload_id = NEW.payload_id
       ) THEN
        RETURN NEW;
    END IF;
    RAISE EXCEPTION 'Payload state transition is invalid' USING ERRCODE = '55000';
END;
$$;
CREATE TRIGGER payload_header_state BEFORE UPDATE OR DELETE ON payload_headers
    FOR EACH ROW EXECUTE FUNCTION payload_header_guard();

CREATE FUNCTION payload_lineage_guard() RETURNS trigger LANGUAGE plpgsql
SET search_path FROM CURRENT AS $$
DECLARE header payload_headers;
BEGIN
    SELECT * INTO header FROM payload_headers
        WHERE bot_id = NEW.bot_id AND payload_id = NEW.payload_id;
    IF header.payload_id IS NULL OR header.state <> 'ACTIVE'
       OR EXISTS (
           SELECT 1 FROM payload_bodies
           WHERE bot_id = NEW.bot_id AND payload_id = NEW.payload_id
       ) THEN
        RAISE EXCEPTION 'Payload lineage is already frozen' USING ERRCODE = '55000';
    END IF;
    RETURN NEW;
END;
$$;
CREATE TRIGGER derivation_admission BEFORE INSERT ON payload_derivations
    FOR EACH ROW EXECUTE FUNCTION payload_lineage_guard();

CREATE FUNCTION payload_body_guard() RETURNS trigger LANGUAGE plpgsql
SET search_path FROM CURRENT AS $$
DECLARE header payload_headers;
DECLARE current_epoch bigint;
DECLARE manifest_count integer;
DECLARE actual_sources integer;
DECLARE actual_parents integer;
BEGIN
    IF TG_OP = 'UPDATE' THEN
        RAISE EXCEPTION 'Payload bodies are write once' USING ERRCODE = '55000';
    END IF;
    SELECT * INTO header FROM payload_headers
        WHERE bot_id = COALESCE(NEW.bot_id, OLD.bot_id)
          AND payload_id = COALESCE(NEW.payload_id, OLD.payload_id);
    IF TG_OP = 'DELETE' THEN
        IF header.state = 'REVOKED' AND header.revoke_operation_id IS NOT NULL THEN
            RETURN OLD;
        END IF;
        RAISE EXCEPTION 'Payload removal requires committed revocation' USING ERRCODE = '55000';
    END IF;
    SELECT deletion_epoch INTO current_epoch FROM deletion_clock
        WHERE bot_id = NEW.bot_id FOR SHARE;
    SELECT item_count INTO manifest_count FROM source_manifests
        WHERE bot_id = NEW.bot_id AND manifest_id = header.source_manifest_id;
    SELECT count(*) INTO actual_sources FROM source_manifest_items
        WHERE bot_id = NEW.bot_id AND manifest_id = header.source_manifest_id;
    SELECT count(*) INTO actual_parents FROM payload_derivations
        WHERE bot_id = NEW.bot_id AND payload_id = NEW.payload_id;
    IF header.payload_id IS NULL
       OR header.state <> 'ACTIVE'
       OR header.accepted_deletion_epoch <> current_epoch
       OR header.source_manifest_digest IS DISTINCT FROM (
           SELECT manifest_digest FROM source_manifests
           WHERE bot_id = NEW.bot_id AND manifest_id = header.source_manifest_id
       )
       OR manifest_count <> actual_sources
       OR header.parent_count <> actual_parents
       OR header.byte_length <> octet_length(NEW.content)
       OR header.content_digest <> 'sha256:' || encode(sha256(NEW.content), 'hex')
       OR EXISTS (
           SELECT 1
           FROM source_manifest_items i
           JOIN forget_source_targets t
             ON t.bot_id = i.bot_id
            AND t.source_component_id = i.source_component_id
            AND t.source_kind = i.source_kind
            AND t.source_id = i.source_id
            AND t.source_revision = i.source_revision
            AND (t.part_id = '*' OR i.part_id = '*' OR t.part_id = i.part_id)
           JOIN forget_requests request
             ON request.bot_id = t.bot_id
            AND request.operation_id = t.operation_id
            AND request.deletion_epoch <= current_epoch
           WHERE i.bot_id = NEW.bot_id AND i.manifest_id = header.source_manifest_id
       )
       OR EXISTS (
           SELECT 1 FROM payload_derivations d
           JOIN payload_headers parent
             ON parent.bot_id = d.bot_id AND parent.payload_id = d.parent_payload_id
           LEFT JOIN payload_bodies body
             ON body.bot_id = parent.bot_id AND body.payload_id = parent.payload_id
           WHERE d.bot_id = NEW.bot_id AND d.payload_id = NEW.payload_id
             AND (parent.state <> 'ACTIVE' OR body.payload_id IS NULL)
       )
       OR EXISTS (
           SELECT 1
           FROM payload_derivations d
           JOIN payload_headers parent
             ON parent.bot_id = d.bot_id AND parent.payload_id = d.parent_payload_id
           JOIN source_manifest_items parent_source
             ON parent_source.bot_id = parent.bot_id
            AND parent_source.manifest_id = parent.source_manifest_id
           WHERE d.bot_id = NEW.bot_id AND d.payload_id = NEW.payload_id
             AND NOT EXISTS (
                 SELECT 1 FROM source_manifest_items child_source
                 WHERE child_source.bot_id = header.bot_id
                   AND child_source.manifest_id = header.source_manifest_id
                   AND child_source.source_component_id = parent_source.source_component_id
                   AND child_source.source_kind = parent_source.source_kind
                   AND child_source.source_id = parent_source.source_id
                   AND child_source.source_revision = parent_source.source_revision
                   AND child_source.part_id = parent_source.part_id
             )
       ) THEN
        RAISE EXCEPTION 'Payload body has no current admission proof' USING ERRCODE = '55000';
    END IF;
    RETURN NEW;
END;
$$;
CREATE TRIGGER payload_body_admission BEFORE INSERT OR UPDATE OR DELETE ON payload_bodies
    FOR EACH ROW EXECUTE FUNCTION payload_body_guard();

INSERT INTO deletion_clock(bot_id)
SELECT bot_id FROM forest_kernel.bot_identity;
