-- The index is derived, but a deleted ref must never be admitted again.
-- A tombstone also covers deletion that arrives before the first upsert.
CREATE TABLE vector_index_refs (
    bot_id text NOT NULL REFERENCES forest_kernel.bot_identity(bot_id),
    ref_id text NOT NULL CHECK (octet_length(ref_id) BETWEEN 1 AND 256),
    space_id text NOT NULL CHECK (octet_length(space_id) BETWEEN 1 AND 256),
    state text NOT NULL CHECK (state IN ('ACTIVE','TOMBSTONED')),
    purpose text CHECK (purpose IN ('DOCUMENT','QUERY')),
    input_digest text CHECK (input_digest ~ '^sha256:[a-f0-9]{64}$'),
    replay_locator jsonb,
    vector_digest text CHECK (vector_digest ~ '^sha256:[a-f0-9]{64}$'),
    tombstoned_at timestamptz,
    next_sweep_at timestamptz,
    sweep_count integer NOT NULL DEFAULT 0 CHECK (sweep_count >= 0),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (bot_id,ref_id),
    CHECK ((state='ACTIVE') = (input_digest IS NOT NULL)),
    CHECK ((state='ACTIVE') = (purpose IS NOT NULL)),
    CHECK ((state='ACTIVE') = (replay_locator IS NOT NULL)),
    CHECK ((state='ACTIVE') = (vector_digest IS NOT NULL)),
    CHECK ((state='TOMBSTONED') = (tombstoned_at IS NOT NULL)),
    CHECK ((state='TOMBSTONED') = (next_sweep_at IS NOT NULL))
);
CREATE INDEX vector_index_refs_sweep_idx ON vector_index_refs(
    bot_id,next_sweep_at,ref_id
) WHERE state='TOMBSTONED';

-- Milvus names are deterministic, but a dropped and recreated collection has
-- a different server identity. Keep the binding history in the package store.
CREATE TABLE vector_collection_bindings (
    bot_id text NOT NULL REFERENCES forest_kernel.bot_identity(bot_id),
    binding_revision bigint NOT NULL CHECK (binding_revision > 0),
    space_id text NOT NULL CHECK (octet_length(space_id) BETWEEN 1 AND 256),
    collection_name text NOT NULL CHECK (octet_length(collection_name) BETWEEN 1 AND 256),
    collection_id bigint NOT NULL CHECK (collection_id > 0),
    bound_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (bot_id,binding_revision)
);
CREATE FUNCTION guard_vector_collection_binding() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    RAISE EXCEPTION 'Vector collection binding is immutable' USING ERRCODE='55000';
END;
$$;
CREATE TRIGGER vector_collection_binding_immutable
    BEFORE UPDATE OR DELETE ON vector_collection_bindings
    FOR EACH ROW EXECUTE FUNCTION guard_vector_collection_binding();

CREATE FUNCTION guard_vector_index_ref() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF ROW(NEW.bot_id,NEW.ref_id,NEW.space_id,NEW.created_at) IS DISTINCT FROM
       ROW(OLD.bot_id,OLD.ref_id,OLD.space_id,OLD.created_at) THEN
        RAISE EXCEPTION 'Vector ref identity is immutable' USING ERRCODE='55000';
    END IF;
    IF OLD.state='ACTIVE' AND NEW.state='TOMBSTONED'
       AND NEW.purpose IS NULL AND NEW.input_digest IS NULL
       AND NEW.replay_locator IS NULL
       AND NEW.vector_digest IS NULL AND NEW.tombstoned_at IS NOT NULL
       AND NEW.next_sweep_at IS NOT NULL AND NEW.sweep_count=0 THEN
        RETURN NEW;
    END IF;
    IF OLD.state='TOMBSTONED' AND NEW.state='TOMBSTONED'
       AND ROW(NEW.input_digest,NEW.tombstoned_at) IS NOT DISTINCT FROM
           ROW(OLD.input_digest,OLD.tombstoned_at)
       AND NEW.sweep_count=OLD.sweep_count+1
       AND NEW.next_sweep_at>OLD.next_sweep_at THEN
        RETURN NEW;
    END IF;
    IF NEW IS NOT DISTINCT FROM OLD THEN RETURN NEW; END IF;
    RAISE EXCEPTION 'Deleted vector ref cannot be reactivated' USING ERRCODE='55000';
END;
$$;
CREATE TRIGGER vector_index_ref_guard BEFORE UPDATE OR DELETE ON vector_index_refs
    FOR EACH ROW EXECUTE FUNCTION guard_vector_index_ref();
