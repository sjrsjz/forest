CREATE TABLE vector_index_identity (
    bot_id text PRIMARY KEY,
    initialization_operation_id text NOT NULL
        CHECK (octet_length(initialization_operation_id) BETWEEN 1 AND 256),
    input_digest text NOT NULL CHECK (input_digest ~ '^sha256:[a-f0-9]{64}$'),
    space_id text NOT NULL CHECK (octet_length(space_id) BETWEEN 1 AND 256),
    dimensions integer NOT NULL CHECK (dimensions BETWEEN 1 AND 4096),
    metric text NOT NULL CHECK (metric='COSINE'),
    collection_name text NOT NULL CHECK (collection_name ~ '^[a-z][a-z0-9_]{0,254}$'),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (bot_id,initialization_operation_id),
    UNIQUE (bot_id,space_id),
    UNIQUE (collection_name)
);

-- The index is derived, but a deleted ref must never be admitted again.
-- A tombstone also covers deletion that arrives before the first upsert.
CREATE TABLE vector_index_refs (
    bot_id text NOT NULL REFERENCES forest_kernel.bot_identity(bot_id),
    ref_id text NOT NULL CHECK (octet_length(ref_id) BETWEEN 1 AND 256),
    state text NOT NULL CHECK (state IN ('ACTIVE','TOMBSTONED')),
    input_digest text CHECK (input_digest ~ '^sha256:[a-f0-9]{64}$'),
    tombstoned_at timestamptz,
    next_sweep_at timestamptz,
    sweep_count integer NOT NULL DEFAULT 0 CHECK (sweep_count >= 0),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (bot_id,ref_id),
    CHECK ((state='ACTIVE') = (input_digest IS NOT NULL)),
    CHECK ((state='TOMBSTONED') = (tombstoned_at IS NOT NULL)),
    CHECK ((state='TOMBSTONED') = (next_sweep_at IS NOT NULL))
);
CREATE INDEX vector_index_refs_sweep_idx ON vector_index_refs(
    bot_id,next_sweep_at,ref_id
) WHERE state='TOMBSTONED';

CREATE FUNCTION guard_vector_index_ref() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF ROW(NEW.bot_id,NEW.ref_id,NEW.created_at) IS DISTINCT FROM
       ROW(OLD.bot_id,OLD.ref_id,OLD.created_at) THEN
        RAISE EXCEPTION 'Vector ref identity is immutable' USING ERRCODE='55000';
    END IF;
    IF OLD.state='ACTIVE' AND NEW.state='TOMBSTONED'
       AND NEW.input_digest IS NULL AND NEW.tombstoned_at IS NOT NULL
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
