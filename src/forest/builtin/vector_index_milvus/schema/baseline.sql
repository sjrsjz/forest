-- The index is derived, but a deleted ref must never be admitted again.
-- A tombstone also covers deletion that arrives before the first upsert.
CREATE TABLE vector_index_refs (
    bot_id text NOT NULL REFERENCES forest_kernel.bot_identity(bot_id),
    ref_id text NOT NULL CHECK (octet_length(ref_id) BETWEEN 1 AND 256),
    space_id text NOT NULL CHECK (octet_length(space_id) BETWEEN 1 AND 256),
    state text NOT NULL CHECK (state IN ('PENDING','ACTIVE','TOMBSTONED')),
    purpose text CHECK (purpose IN ('DOCUMENT','QUERY')),
    input_digest text CHECK (input_digest ~ '^sha256:[a-f0-9]{64}$'),
    replay_locator jsonb,
    vector_digest text CHECK (vector_digest ~ '^sha256:[a-f0-9]{64}$'),
    physical_digest text CHECK (physical_digest ~ '^f32-sha256:[a-f0-9]{64}$'),
    published_at timestamptz,
    tombstoned_at timestamptz,
    next_sweep_at timestamptz,
    sweep_count integer NOT NULL DEFAULT 0 CHECK (sweep_count >= 0),
    sweep_claim_token text CHECK (octet_length(sweep_claim_token) BETWEEN 1 AND 256),
    sweep_lease_deadline timestamptz,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (bot_id,ref_id),
    CHECK ((state<>'TOMBSTONED') = (input_digest IS NOT NULL)),
    CHECK ((state<>'TOMBSTONED') = (purpose IS NOT NULL)),
    CHECK ((state<>'TOMBSTONED') = (replay_locator IS NOT NULL)),
    CHECK ((state<>'TOMBSTONED') = (vector_digest IS NOT NULL)),
    CHECK ((state<>'TOMBSTONED') = (physical_digest IS NOT NULL)),
    CHECK ((state='ACTIVE') = (published_at IS NOT NULL)),
    CHECK ((state='TOMBSTONED') = (tombstoned_at IS NOT NULL)),
    CHECK ((state='TOMBSTONED') = (next_sweep_at IS NOT NULL)),
    CHECK ((sweep_claim_token IS NULL) = (sweep_lease_deadline IS NULL)),
    CHECK (state='TOMBSTONED' OR sweep_claim_token IS NULL)
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
    dimensions integer NOT NULL CHECK (dimensions BETWEEN 1 AND 4096),
    metric text NOT NULL CHECK (metric='COSINE'),
    bound_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (bot_id,binding_revision)
);
CREATE TABLE vector_collection_health (
    bot_id text PRIMARY KEY REFERENCES forest_kernel.bot_identity(bot_id),
    binding_revision bigint NOT NULL CHECK (binding_revision > 0),
    state text NOT NULL CHECK (state IN ('READY','REBUILD_REQUIRED')),
    reason text,
    observed_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    FOREIGN KEY (bot_id,binding_revision) REFERENCES vector_collection_bindings(bot_id,binding_revision),
    CHECK ((state='READY') = (reason IS NULL))
);
CREATE TABLE vector_collection_start_claims (
    bot_id text PRIMARY KEY REFERENCES forest_kernel.bot_identity(bot_id),
    space_id text NOT NULL CHECK (octet_length(space_id) BETWEEN 1 AND 256),
    collection_name text NOT NULL CHECK (octet_length(collection_name) BETWEEN 1 AND 256),
    dimensions integer NOT NULL CHECK (dimensions BETWEEN 1 AND 4096),
    metric text NOT NULL CHECK (metric='COSINE'),
    claim_token text NOT NULL CHECK (octet_length(claim_token) BETWEEN 1 AND 256),
    lease_deadline timestamptz NOT NULL
);
-- One transactional counter per Bot orders committed ref transitions. Its
-- row lock serializes concurrent publications and tombstones without a SQL
-- transaction spanning Milvus I/O.
CREATE TABLE vector_ref_clocks (
    bot_id text PRIMARY KEY REFERENCES forest_kernel.bot_identity(bot_id),
    current_seq bigint NOT NULL DEFAULT 0 CHECK (current_seq >= 0)
);
CREATE TABLE vector_ref_changes (
    bot_id text NOT NULL REFERENCES forest_kernel.bot_identity(bot_id),
    mutation_seq bigint NOT NULL CHECK (mutation_seq > 0),
    ref_id text NOT NULL CHECK (octet_length(ref_id) BETWEEN 1 AND 256),
    space_id text NOT NULL CHECK (octet_length(space_id) BETWEEN 1 AND 256),
    state text NOT NULL CHECK (state IN ('ACTIVE','TOMBSTONED')),
    purpose text CHECK (purpose IN ('DOCUMENT','QUERY')),
    input_digest text CHECK (input_digest ~ '^sha256:[a-f0-9]{64}$'),
    replay_locator jsonb,
    vector_digest text CHECK (vector_digest ~ '^sha256:[a-f0-9]{64}$'),
    physical_digest text CHECK (physical_digest ~ '^f32-sha256:[a-f0-9]{64}$'),
    changed_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (bot_id,mutation_seq),
    FOREIGN KEY (bot_id,ref_id) REFERENCES vector_index_refs(bot_id,ref_id),
    CHECK ((state='ACTIVE') = (purpose IS NOT NULL)),
    CHECK ((state='ACTIVE') = (input_digest IS NOT NULL)),
    CHECK ((state='ACTIVE') = (replay_locator IS NOT NULL)),
    CHECK ((state='ACTIVE') = (vector_digest IS NOT NULL)),
    CHECK ((state='ACTIVE') = (physical_digest IS NOT NULL))
);
CREATE FUNCTION guard_vector_ref_change() RETURNS trigger
LANGUAGE plpgsql SET search_path = pg_catalog AS $$
BEGIN
    RAISE EXCEPTION 'Vector ref change is immutable' USING ERRCODE='55000';
END;
$$;
CREATE TRIGGER vector_ref_change_immutable
    BEFORE UPDATE OR DELETE ON vector_ref_changes
    FOR EACH ROW EXECUTE FUNCTION guard_vector_ref_change();

CREATE FUNCTION vector_append_ref_change(
    requested_bot text, requested_ref text, requested_space text,
    next_state text, next_purpose text, next_digest text,
    next_replay jsonb, next_vector_digest text, next_physical_digest text
) RETURNS bigint LANGUAGE plpgsql VOLATILE SET search_path = pg_catalog AS $$
DECLARE
    next_seq bigint;
BEGIN
    INSERT INTO __forest_module_schema__.vector_ref_clocks(bot_id)
    VALUES(requested_bot) ON CONFLICT (bot_id) DO NOTHING;
    UPDATE __forest_module_schema__.vector_ref_clocks
    SET current_seq=current_seq+1 WHERE bot_id=requested_bot
    RETURNING current_seq INTO next_seq;
    INSERT INTO __forest_module_schema__.vector_ref_changes(
        bot_id,mutation_seq,ref_id,space_id,state,purpose,input_digest,
        replay_locator,vector_digest,physical_digest
    ) VALUES(
        requested_bot,next_seq,requested_ref,requested_space,next_state,
        next_purpose,next_digest,next_replay,next_vector_digest,next_physical_digest
    );
    RETURN next_seq;
END;
$$;

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
    IF OLD.state='PENDING' AND NEW.state='ACTIVE'
       AND ROW(NEW.purpose,NEW.input_digest,NEW.replay_locator,NEW.vector_digest,NEW.physical_digest)
           IS NOT DISTINCT FROM
           ROW(OLD.purpose,OLD.input_digest,OLD.replay_locator,OLD.vector_digest,OLD.physical_digest)
       AND NEW.published_at IS NOT NULL
       AND NEW.tombstoned_at IS NULL AND NEW.next_sweep_at IS NULL
       AND NEW.sweep_count=0 THEN
        RETURN NEW;
    END IF;
    IF OLD.state IN ('PENDING','ACTIVE') AND NEW.state='TOMBSTONED'
       AND NEW.purpose IS NULL AND NEW.input_digest IS NULL
       AND NEW.replay_locator IS NULL
       AND NEW.vector_digest IS NULL AND NEW.physical_digest IS NULL
       AND NEW.published_at IS NULL
       AND NEW.tombstoned_at IS NOT NULL
       AND NEW.next_sweep_at IS NOT NULL AND NEW.sweep_count=0
       AND NEW.sweep_claim_token IS NULL
       AND NEW.sweep_lease_deadline IS NULL THEN
        RETURN NEW;
    END IF;
    IF OLD.state='TOMBSTONED' AND NEW.state='TOMBSTONED'
       AND ROW(NEW.purpose,NEW.input_digest,NEW.replay_locator,
               NEW.vector_digest,NEW.physical_digest,NEW.tombstoned_at) IS NOT DISTINCT FROM
           ROW(OLD.purpose,OLD.input_digest,OLD.replay_locator,
               OLD.vector_digest,OLD.physical_digest,OLD.tombstoned_at)
       AND (
           (NEW.sweep_count=OLD.sweep_count
            AND NEW.next_sweep_at=OLD.next_sweep_at
            AND NEW.sweep_claim_token IS NOT NULL
            AND NEW.sweep_lease_deadline>clock_timestamp()
            AND (OLD.sweep_claim_token IS NULL
                 OR OLD.sweep_lease_deadline<=clock_timestamp()
                 OR NEW.sweep_claim_token=OLD.sweep_claim_token))
           OR
           (NEW.sweep_count=OLD.sweep_count+1
            AND OLD.sweep_claim_token IS NOT NULL
            AND OLD.sweep_lease_deadline>clock_timestamp()
            AND NEW.next_sweep_at>OLD.next_sweep_at
            AND NEW.sweep_claim_token IS NULL
            AND NEW.sweep_lease_deadline IS NULL)
       ) THEN
        RETURN NEW;
    END IF;
    IF NEW IS NOT DISTINCT FROM OLD THEN RETURN NEW; END IF;
    RAISE EXCEPTION 'Deleted vector ref cannot be reactivated' USING ERRCODE='55000';
END;
$$;
CREATE TRIGGER vector_index_ref_guard BEFORE UPDATE OR DELETE ON vector_index_refs
    FOR EACH ROW EXECUTE FUNCTION guard_vector_index_ref();


-- The package owns recurring tombstone cleanup. A lease covers one bounded
-- batch; a failed or unknown physical delete remains eligible for replay.
CREATE TABLE vector_sweep_settlements (
    bot_id text NOT NULL REFERENCES forest_kernel.bot_identity(bot_id),
    claim_token text NOT NULL CHECK (octet_length(claim_token) BETWEEN 1 AND 256),
    input_payload jsonb NOT NULL,
    result_payload jsonb NOT NULL,
    settled_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (bot_id,claim_token)
);
CREATE INDEX vector_sweep_settlements_gc_idx ON vector_sweep_settlements(
    bot_id,settled_at,claim_token
);
CREATE FUNCTION guard_vector_sweep_settlement() RETURNS trigger
LANGUAGE plpgsql SET search_path = pg_catalog AS $$
BEGIN
    IF TG_OP='DELETE'
       AND OLD.settled_at<clock_timestamp()-interval '48 hours' THEN
        RETURN OLD;
    END IF;
    RAISE EXCEPTION 'Tombstone sweep settlement is immutable until expiry'
        USING ERRCODE='55000';
END;
$$;
CREATE TRIGGER vector_sweep_settlement_guard
    BEFORE UPDATE OR DELETE ON vector_sweep_settlements
    FOR EACH ROW EXECUTE FUNCTION guard_vector_sweep_settlement();

CREATE INDEX vector_index_refs_sweep_claim_idx ON vector_index_refs(
    bot_id,sweep_claim_token
) WHERE sweep_claim_token IS NOT NULL;

CREATE FUNCTION vector_claim_tombstone_sweep(context jsonb, input jsonb)
RETURNS jsonb LANGUAGE plpgsql VOLATILE SET search_path = pg_catalog AS $$
DECLARE
    requested_bot text := context->>'bot_id';
    requested_token text := input->>'claim_token';
    requested_lease integer := (input->>'lease_seconds')::integer;
    selected_space text;
    last_space text;
    selected_ids text[];
    binding __forest_module_schema__.vector_collection_bindings%ROWTYPE;
    work_key text;
BEGIN
    IF input->>'protocol' IS DISTINCT FROM 'forest.owned-work-claim-request/1'
       OR requested_bot IS NULL
       OR requested_token IS NULL
       OR octet_length(requested_token) NOT BETWEEN 1 AND 256
       OR requested_lease IS NULL OR requested_lease NOT BETWEEN 3 AND 86400 THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object('code','VECTOR_SWEEP_CLAIM_INVALID',
                'message','Tombstone sweep claim is invalid','status',422));
    END IF;
    -- Keep only the replay horizon for idempotent settle receipts. Tombstones
    -- remain permanent; this bounded cleanup cannot delete the ref fence.
    DELETE FROM __forest_module_schema__.vector_sweep_settlements receipt
    WHERE (receipt.bot_id,receipt.claim_token) IN (
        SELECT old.bot_id,old.claim_token
        FROM __forest_module_schema__.vector_sweep_settlements old
        WHERE old.bot_id=requested_bot
          AND old.settled_at<clock_timestamp()-interval '48 hours'
        ORDER BY old.settled_at,old.claim_token
        LIMIT 128 FOR UPDATE SKIP LOCKED
    );
    PERFORM pg_advisory_xact_lock(hashtextextended(
        'vector-sweep-claim:'||requested_bot||':'||requested_token,0));
    SELECT array_agg(ref_id ORDER BY ref_id),min(space_id),max(space_id)
      INTO selected_ids,selected_space,last_space
    FROM __forest_module_schema__.vector_index_refs
    WHERE bot_id=requested_bot AND state='TOMBSTONED'
      AND sweep_claim_token=requested_token
      AND sweep_lease_deadline>clock_timestamp();
    IF selected_ids IS NULL THEN
        SELECT space_id INTO selected_space
        FROM __forest_module_schema__.vector_index_refs
        WHERE bot_id=requested_bot AND state='TOMBSTONED'
          AND next_sweep_at<=clock_timestamp()
          AND (sweep_lease_deadline IS NULL
               OR sweep_lease_deadline<=clock_timestamp())
        ORDER BY next_sweep_at,ref_id LIMIT 1;
        IF selected_space IS NULL THEN
            RETURN jsonb_build_object('protocol','forest.domain-result/1',
                                      'ok',true,'value',NULL);
        END IF;
        -- Do not lease tombstones when no collection can receive the prune.
        IF NOT EXISTS(
            SELECT 1 FROM __forest_module_schema__.vector_collection_bindings
            WHERE bot_id=requested_bot AND space_id=selected_space
        ) THEN
            RETURN jsonb_build_object('protocol','forest.domain-result/1',
                                      'ok',true,'value',NULL);
        END IF;
        WITH due AS (
            SELECT ref_id FROM __forest_module_schema__.vector_index_refs
            WHERE bot_id=requested_bot AND space_id=selected_space
              AND state='TOMBSTONED' AND next_sweep_at<=clock_timestamp()
              AND (sweep_lease_deadline IS NULL
                   OR sweep_lease_deadline<=clock_timestamp())
            ORDER BY next_sweep_at,ref_id LIMIT 128 FOR UPDATE SKIP LOCKED
        ), claimed AS (
            UPDATE __forest_module_schema__.vector_index_refs r
            SET sweep_claim_token=requested_token,
                sweep_lease_deadline=clock_timestamp()
                    +(requested_lease * interval '1 second')
            WHERE r.bot_id=requested_bot AND r.ref_id IN (SELECT ref_id FROM due)
            RETURNING r.ref_id
        )
        SELECT array_agg(ref_id ORDER BY ref_id) INTO selected_ids FROM claimed;
        IF selected_ids IS NULL THEN
            RETURN jsonb_build_object('protocol','forest.domain-result/1',
                                      'ok',true,'value',NULL);
        END IF;
    ELSIF selected_space IS DISTINCT FROM last_space THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object('code','VECTOR_SWEEP_SPACE_CONFLICT',
                'message','One sweep lease spans multiple vector spaces','status',409));
    END IF;
    SELECT * INTO binding
    FROM __forest_module_schema__.vector_collection_bindings
    WHERE bot_id=requested_bot AND space_id=selected_space
    ORDER BY binding_revision DESC LIMIT 1;
    IF NOT FOUND THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1',
                                  'ok',true,'value',NULL);
    END IF;
    work_key := 'vector-sweep:' ||
        encode(sha256(convert_to(requested_token,'UTF8')),'hex');
    RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',true,
        'value',jsonb_build_object(
            'protocol','forest.owned-work-claim/1',
            'work_id',work_key,'claim_token',requested_token,'attempt',1,
            'operation_id',work_key,
            'causal_root_kind','vector.tombstone','causal_root_id',work_key,
            'payload',jsonb_build_object(
                'ref_ids',to_jsonb(selected_ids),'space_id',selected_space,
                'binding',jsonb_build_object(
                    'binding_revision',binding.binding_revision,
                    'collection_id',binding.collection_id,
                    'collection_name',binding.collection_name))));
END;
$$;

CREATE FUNCTION vector_settle_tombstone_sweep(context jsonb, input jsonb)
RETURNS jsonb LANGUAGE plpgsql VOLATILE SET search_path = pg_catalog AS $$
DECLARE
    requested_bot text := context->>'bot_id';
    requested_token text := input->>'claim_token';
    requested_work text := input->>'work_id';
    requested_outcome jsonb := input->'outcome';
    expected_work text;
    existing_input jsonb;
    existing_result jsonb;
    claimed_count integer;
    earliest_lease timestamptz;
    result_value jsonb;
BEGIN
    IF input->>'protocol' IS DISTINCT FROM 'forest.owned-work-settle-request/1'
       OR requested_bot IS NULL OR requested_token IS NULL
       OR octet_length(requested_token) NOT BETWEEN 1 AND 256
       OR requested_work IS NULL OR input->>'attempt' IS DISTINCT FROM '1'
       OR requested_outcome->>'status' IS NULL
       OR requested_outcome->>'status' NOT IN ('SUCCEEDED','FAILED') THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object('code','VECTOR_SWEEP_SETTLE_INVALID',
                'message','Tombstone sweep settlement is invalid','status',422));
    END IF;
    expected_work := 'vector-sweep:' ||
        encode(sha256(convert_to(requested_token,'UTF8')),'hex');
    IF requested_work IS DISTINCT FROM expected_work THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object('code','VECTOR_SWEEP_SETTLE_INVALID',
                'message','Tombstone sweep work identity differs','status',422));
    END IF;
    PERFORM pg_advisory_xact_lock(hashtextextended(
        'vector-sweep-claim:'||requested_bot||':'||requested_token,0));
    SELECT input_payload,result_payload INTO existing_input,existing_result
    FROM __forest_module_schema__.vector_sweep_settlements
    WHERE bot_id=requested_bot AND claim_token=requested_token;
    IF FOUND THEN
        IF existing_input=input THEN RETURN existing_result; END IF;
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object('code','VECTOR_SWEEP_SETTLE_CONFLICT',
                'message','Claim token was settled with another outcome','status',409));
    END IF;
    SELECT count(*),min(sweep_lease_deadline)
      INTO claimed_count,earliest_lease
    FROM (
        SELECT sweep_lease_deadline
        FROM __forest_module_schema__.vector_index_refs
        WHERE bot_id=requested_bot AND state='TOMBSTONED'
          AND sweep_claim_token=requested_token
        FOR UPDATE
    ) locked;
    IF claimed_count=0 OR earliest_lease<=clock_timestamp() THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object('code','VECTOR_SWEEP_CLAIM_STALE',
                'message','Tombstone sweep lease is no longer current','status',409));
    END IF;
    UPDATE __forest_module_schema__.vector_index_refs
    SET sweep_count=sweep_count+1,
        next_sweep_at=clock_timestamp()
            + ((CASE WHEN requested_outcome->>'status'='SUCCEEDED'
                THEN LEAST(30 * power(2,LEAST(sweep_count+1,12)),86400)
                ELSE 30 END)::double precision * interval '1 second'),
        sweep_claim_token=NULL,sweep_lease_deadline=NULL
    WHERE bot_id=requested_bot AND state='TOMBSTONED'
      AND sweep_claim_token=requested_token;
    result_value := jsonb_build_object('protocol','forest.domain-result/1',
        'ok',true,'value',jsonb_build_object(
            'protocol','forest.owned-work-settled/1','work_id',requested_work));
    INSERT INTO __forest_module_schema__.vector_sweep_settlements(
        bot_id,claim_token,input_payload,result_payload
    ) VALUES(requested_bot,requested_token,input,result_value);
    RETURN result_value;
END;
$$;

-- Ref admission is package-owned and never holds a SQL transaction across
-- Milvus I/O. PENDING records preserve exact replay evidence after a crash.
CREATE FUNCTION vector_ref_reserve(context jsonb, input jsonb)
RETURNS jsonb LANGUAGE plpgsql VOLATILE SET search_path = pg_catalog AS $$
DECLARE
    requested_bot text := context->>'bot_id';
    requested_ref text := input->>'ref_id';
    requested_space text := input->>'space_id';
    requested_purpose text := input->>'purpose';
    requested_digest text := input->>'source_input_digest';
    requested_physical_digest text := input->>'physical_digest';
    requested_replay jsonb := input->'replay';
    requested_vector_digest text := requested_replay->>'vector_digest';
    existing __forest_module_schema__.vector_index_refs%ROWTYPE;
    binding __forest_module_schema__.vector_collection_bindings%ROWTYPE;
    result_state text;
BEGIN
    IF requested_bot IS NULL
       OR requested_ref IS NULL OR octet_length(requested_ref) NOT BETWEEN 1 AND 256
       OR requested_space IS NULL OR octet_length(requested_space) NOT BETWEEN 1 AND 256
       OR requested_purpose IS NULL OR requested_purpose NOT IN ('DOCUMENT','QUERY')
       OR requested_digest IS NULL OR requested_digest !~ '^sha256:[a-f0-9]{64}$'
       OR jsonb_typeof(requested_replay) IS DISTINCT FROM 'object'
       OR requested_vector_digest IS NULL OR requested_vector_digest !~ '^sha256:[a-f0-9]{64}$'
       OR requested_physical_digest IS NULL
       OR requested_physical_digest !~ '^f32-sha256:[a-f0-9]{64}$' THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object('code','VECTOR_INDEX_INPUT_INVALID',
                'message','Vector ref reservation is invalid','status',422));
    END IF;
    PERFORM pg_advisory_xact_lock_shared(hashtextextended(
        'vector-collection:'||requested_bot,0));
    IF EXISTS(
        SELECT 1 FROM __forest_module_schema__.vector_rebuild_work
        WHERE bot_id=requested_bot AND state='PUBLISHING'
    ) THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object('code','VECTOR_INDEX_SWITCHING',
                'message','Vector binding is publishing','status',503));
    END IF;
    IF EXISTS(
        SELECT 1 FROM __forest_module_schema__.vector_collection_start_claims
        WHERE bot_id=requested_bot AND lease_deadline>clock_timestamp()
    ) THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object('code','VECTOR_INDEX_COLLECTION_STARTING',
                'message','Collection startup has not settled','status',503));
    END IF;
    SELECT b.* INTO binding
    FROM __forest_module_schema__.vector_collection_health h
    JOIN __forest_module_schema__.vector_collection_bindings b
      ON b.bot_id=h.bot_id AND b.binding_revision=h.binding_revision
    WHERE h.bot_id=requested_bot AND h.state='READY'
      AND b.space_id=requested_space;
    IF NOT FOUND THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object('code','VECTOR_INDEX_REBUILD_REQUIRED',
                'message','Vector collection is not ready','status',503));
    END IF;
    PERFORM pg_advisory_xact_lock(hashtextextended(
        'vector-ref:'||requested_bot||':'||requested_ref,0));
    SELECT * INTO existing FROM __forest_module_schema__.vector_index_refs
    WHERE bot_id=requested_bot AND ref_id=requested_ref FOR UPDATE;
    IF NOT FOUND THEN
        INSERT INTO __forest_module_schema__.vector_index_refs(
            bot_id,ref_id,space_id,state,purpose,input_digest,
            replay_locator,vector_digest,physical_digest
        ) VALUES(
            requested_bot,requested_ref,requested_space,'PENDING',
            requested_purpose,requested_digest,requested_replay,
            requested_vector_digest,requested_physical_digest
        );
        result_state := 'PENDING';
    ELSE
        IF existing.state='TOMBSTONED' THEN
            RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
                'error',jsonb_build_object('code','VECTOR_INDEX_REF_DELETED',
                    'message','Deleted vector ref cannot be published','status',409));
        END IF;
        IF existing.space_id IS DISTINCT FROM requested_space
           OR existing.purpose IS DISTINCT FROM requested_purpose
           OR existing.input_digest IS DISTINCT FROM requested_digest
           OR existing.replay_locator IS DISTINCT FROM requested_replay
           OR existing.vector_digest IS DISTINCT FROM requested_vector_digest
           OR existing.physical_digest IS DISTINCT FROM requested_physical_digest THEN
            RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
                'error',jsonb_build_object('code','VECTOR_INDEX_REF_CONFLICT',
                    'message','Vector ref was bound to other data','status',409));
        END IF;
        result_state := existing.state;
    END IF;
    RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',true,
        'value',jsonb_build_object('ref_id',requested_ref,'space_id',requested_space,
                                   'input_digest',requested_digest,'state',result_state,
                                   'binding',jsonb_build_object(
                                       'binding_revision',binding.binding_revision,
                                       'collection_id',binding.collection_id,
                                       'collection_name',binding.collection_name)));
END;
$$;

CREATE FUNCTION vector_ref_activate(context jsonb, input jsonb)
RETURNS jsonb LANGUAGE plpgsql VOLATILE SET search_path = pg_catalog AS $$
DECLARE
    requested_bot text := context->>'bot_id';
    requested_ref text := input->>'ref_id';
    requested_digest text := input->>'input_digest';
    requested_binding jsonb := input->'binding';
    existing __forest_module_schema__.vector_index_refs%ROWTYPE;
BEGIN
    IF requested_bot IS NULL
       OR requested_ref IS NULL OR octet_length(requested_ref) NOT BETWEEN 1 AND 256
       OR requested_digest IS NULL OR requested_digest !~ '^sha256:[a-f0-9]{64}$' THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object('code','VECTOR_INDEX_INPUT_INVALID',
                'message','Vector ref activation is invalid','status',422));
    END IF;
    PERFORM pg_advisory_xact_lock_shared(hashtextextended(
        'vector-collection:'||requested_bot,0));
    IF NOT EXISTS(
        SELECT 1 FROM __forest_module_schema__.vector_collection_health h
        JOIN __forest_module_schema__.vector_collection_bindings b
          ON b.bot_id=h.bot_id AND b.binding_revision=h.binding_revision
        WHERE h.bot_id=requested_bot AND h.state='READY'
          AND b.binding_revision=(requested_binding->>'binding_revision')::bigint
          AND b.collection_id=(requested_binding->>'collection_id')::bigint
          AND b.collection_name=(requested_binding->>'collection_name')
    ) THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object('code','VECTOR_INDEX_BINDING_STALE',
                'message','Vector binding changed before publication','status',503));
    END IF;
    PERFORM pg_advisory_xact_lock(hashtextextended(
        'vector-ref:'||requested_bot||':'||requested_ref,0));
    SELECT * INTO existing FROM __forest_module_schema__.vector_index_refs
    WHERE bot_id=requested_bot AND ref_id=requested_ref FOR UPDATE;
    IF NOT FOUND THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object('code','VECTOR_INDEX_REF_MISSING',
                'message','Vector ref reservation is missing','status',409));
    END IF;
    IF existing.state='TOMBSTONED' THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object('code','VECTOR_INDEX_REF_DELETED',
                'message','Deleted vector ref cannot be published','status',409));
    END IF;
    IF existing.input_digest IS DISTINCT FROM requested_digest THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object('code','VECTOR_INDEX_REF_CONFLICT',
                'message','Vector ref was bound to other data','status',409));
    END IF;
    IF existing.state='PENDING' THEN
        UPDATE __forest_module_schema__.vector_index_refs
        SET state='ACTIVE',published_at=clock_timestamp()
        WHERE bot_id=requested_bot AND ref_id=requested_ref;
        PERFORM __forest_module_schema__.vector_append_ref_change(
            requested_bot,requested_ref,existing.space_id,'ACTIVE',
            existing.purpose,existing.input_digest,existing.replay_locator,
            existing.vector_digest,existing.physical_digest
        );
        -- New published refs need not wait for the periodic audit interval.
        UPDATE __forest_module_schema__.vector_audit_work
        SET next_at=LEAST(next_at,clock_timestamp())
        WHERE bot_id=requested_bot;
    END IF;
    RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',true,
        'value',jsonb_build_object('ref_id',requested_ref,
                                   'space_id',existing.space_id,'state','ACTIVE'));
END;
$$;

CREATE FUNCTION vector_ref_tombstone(context jsonb, input jsonb)
RETURNS jsonb LANGUAGE plpgsql VOLATILE SET search_path = pg_catalog AS $$
DECLARE
    requested_bot text := context->>'bot_id';
    requested_space text := input->>'space_id';
    requested_ref text;
    existing __forest_module_schema__.vector_index_refs%ROWTYPE;
BEGIN
    IF requested_bot IS NULL
       OR requested_space IS NULL OR octet_length(requested_space) NOT BETWEEN 1 AND 256
       OR jsonb_typeof(input->'ref_ids') IS DISTINCT FROM 'array' THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object('code','VECTOR_INDEX_INPUT_INVALID',
                'message','Vector tombstone request is invalid','status',422));
    END IF;
    IF jsonb_array_length(input->'ref_ids') NOT BETWEEN 1 AND 1024 THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object('code','VECTOR_INDEX_INPUT_INVALID',
                'message','Vector tombstone request is invalid','status',422));
    END IF;
    -- Validate and lock every ref before mutating any of them. A conflict in
    -- the last list member must not commit earlier tombstones.
    PERFORM pg_advisory_xact_lock_shared(hashtextextended(
        'vector-collection:'||requested_bot,0));
    IF EXISTS(
        SELECT 1 FROM __forest_module_schema__.vector_rebuild_work
        WHERE bot_id=requested_bot AND state='PUBLISHING'
    ) THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object('code','VECTOR_INDEX_SWITCHING',
                'message','Vector binding is publishing','status',503));
    END IF;
    FOR requested_ref IN
        SELECT value FROM jsonb_array_elements_text(input->'ref_ids')
        ORDER BY value
    LOOP
        IF octet_length(requested_ref) NOT BETWEEN 1 AND 256 THEN
            RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
                'error',jsonb_build_object('code','VECTOR_INDEX_INPUT_INVALID',
                    'message','Vector ref identity is invalid','status',422));
        END IF;
        PERFORM pg_advisory_xact_lock(hashtextextended(
            'vector-ref:'||requested_bot||':'||requested_ref,0));
        SELECT * INTO existing FROM __forest_module_schema__.vector_index_refs
        WHERE bot_id=requested_bot AND ref_id=requested_ref FOR UPDATE;
        IF FOUND AND existing.space_id IS DISTINCT FROM requested_space THEN
            RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
                'error',jsonb_build_object('code','VECTOR_INDEX_SPACE_MISMATCH',
                    'message','Vector ref belongs to another space','status',409));
        END IF;
    END LOOP;
    FOR requested_ref IN
        SELECT DISTINCT value FROM jsonb_array_elements_text(input->'ref_ids')
        ORDER BY value
    LOOP
        SELECT * INTO existing FROM __forest_module_schema__.vector_index_refs
        WHERE bot_id=requested_bot AND ref_id=requested_ref FOR UPDATE;
        IF NOT FOUND THEN
            INSERT INTO __forest_module_schema__.vector_index_refs(
                bot_id,ref_id,space_id,state,tombstoned_at,next_sweep_at
            ) VALUES(
                requested_bot,requested_ref,requested_space,'TOMBSTONED',
                clock_timestamp(),clock_timestamp()
            );
            PERFORM __forest_module_schema__.vector_append_ref_change(
                requested_bot,requested_ref,requested_space,'TOMBSTONED',
                NULL,NULL,NULL,NULL,NULL
            );
        ELSIF existing.state<>'TOMBSTONED' THEN
            UPDATE __forest_module_schema__.vector_index_refs
            SET state='TOMBSTONED',purpose=NULL,input_digest=NULL,
                replay_locator=NULL,vector_digest=NULL,physical_digest=NULL,
                published_at=NULL,
                tombstoned_at=clock_timestamp(),next_sweep_at=clock_timestamp()
            WHERE bot_id=requested_bot AND ref_id=requested_ref;
            PERFORM __forest_module_schema__.vector_append_ref_change(
                requested_bot,requested_ref,requested_space,'TOMBSTONED',
                NULL,NULL,NULL,NULL,NULL
            );
        END IF;
    END LOOP;
    RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',true,
        'value',jsonb_build_object('ref_ids',input->'ref_ids',
                                   'space_id',requested_space));
END;
$$;

-- A caller freezes through_seq once, then reads exact committed deltas
-- in bounded pages. Gaps are corruption, never silently treated as completion.
CREATE FUNCTION vector_ref_change_head(context jsonb, input jsonb)
RETURNS jsonb LANGUAGE plpgsql STABLE SET search_path = pg_catalog AS $$
DECLARE
    requested_bot text := context->>'bot_id';
    current_seq bigint;
BEGIN
    SELECT c.current_seq INTO current_seq
    FROM __forest_module_schema__.vector_ref_clocks c
    WHERE c.bot_id=requested_bot;
    RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',true,
        'value',jsonb_build_object('through_seq',COALESCE(current_seq,0)));
END;
$$;

CREATE FUNCTION vector_ref_change_page(context jsonb, input jsonb)
RETURNS jsonb LANGUAGE plpgsql STABLE SET search_path = pg_catalog AS $$
DECLARE
    requested_bot text := context->>'bot_id';
    after_seq bigint := (input->>'after_seq')::bigint;
    through_seq bigint := (input->>'through_seq')::bigint;
    page_limit integer := (input->>'limit')::integer;
    current_seq bigint;
    selected_count integer;
    first_seq bigint;
    last_seq bigint;
    selected_changes jsonb;
BEGIN
    IF requested_bot IS NULL OR after_seq IS NULL OR through_seq IS NULL
       OR page_limit IS NULL OR after_seq<0 OR through_seq<after_seq
       OR page_limit NOT BETWEEN 1 AND 128 THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object('code','VECTOR_REF_CHANGE_INPUT_INVALID',
                'message','Ref change page is invalid','status',422));
    END IF;
    SELECT c.current_seq INTO current_seq
    FROM __forest_module_schema__.vector_ref_clocks c
    WHERE c.bot_id=requested_bot;
    IF through_seq>COALESCE(current_seq,0) THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object('code','VECTOR_REF_CHANGE_WATERMARK_INVALID',
                'message','Ref change watermark exceeds the committed ledger',
                'status',409));
    END IF;
    SELECT count(*),min(mutation_seq),max(mutation_seq),
           COALESCE(jsonb_agg(jsonb_build_object(
               'mutation_seq',mutation_seq,'ref_id',ref_id,
               'space_id',space_id,'state',state,'purpose',purpose,
               'input_digest',input_digest,'replay_locator',replay_locator,
               'vector_digest',vector_digest,'physical_digest',physical_digest
           ) ORDER BY mutation_seq),'[]'::jsonb)
      INTO selected_count,first_seq,last_seq,selected_changes
    FROM (
        SELECT mutation_seq,ref_id,space_id,state,purpose,input_digest,
               replay_locator,vector_digest,physical_digest
        FROM __forest_module_schema__.vector_ref_changes
        WHERE bot_id=requested_bot AND mutation_seq>after_seq
          AND mutation_seq<=through_seq
        ORDER BY mutation_seq LIMIT page_limit
    ) page;
    IF (selected_count=0 AND after_seq<through_seq)
       OR (selected_count>0 AND (
           first_seq<>after_seq+1 OR last_seq<>after_seq+selected_count
       )) THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object('code','VECTOR_REF_CHANGE_GAP',
                'message','Ref change ledger has a gap','status',503));
    END IF;
    RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',true,
        'value',jsonb_build_object('next_seq',COALESCE(last_seq,after_seq),
            'through_seq',through_seq,'changes',selected_changes));
END;
$$;

CREATE FUNCTION vector_ref_require_query(context jsonb, input jsonb)
RETURNS jsonb LANGUAGE plpgsql STABLE SET search_path = pg_catalog AS $$
DECLARE
    requested_bot text := context->>'bot_id';
    requested_ref text := input->>'query_ref_id';
    requested_space text := input->>'space_id';
    binding __forest_module_schema__.vector_collection_bindings%ROWTYPE;
BEGIN
    SELECT b.* INTO binding
    FROM __forest_module_schema__.vector_collection_health h
    JOIN __forest_module_schema__.vector_collection_bindings b
      ON b.bot_id=h.bot_id AND b.binding_revision=h.binding_revision
    WHERE h.bot_id=requested_bot AND h.state='READY'
      AND b.space_id=requested_space;
    IF NOT FOUND THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object('code','VECTOR_INDEX_REBUILD_REQUIRED',
                'message','Vector collection is not ready','status',503));
    END IF;
    IF EXISTS(
        SELECT 1 FROM __forest_module_schema__.vector_index_refs
        WHERE bot_id=requested_bot AND ref_id=requested_ref
          AND space_id=requested_space AND state='ACTIVE' AND purpose='QUERY'
    ) THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',true,
            'value',jsonb_build_object('query_ref_id',requested_ref,
                                       'space_id',requested_space,
                                       'binding',jsonb_build_object(
                                           'binding_revision',binding.binding_revision,
                                           'collection_id',binding.collection_id,
                                           'collection_name',binding.collection_name)));
    END IF;
    RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
        'error',jsonb_build_object('code','VECTOR_INDEX_QUERY_REF_MISSING',
            'message','Query vector ref is unavailable','status',409));
END;
$$;

-- Milvus is derived: only authoritative ACTIVE refs can escape a search.
CREATE FUNCTION vector_ref_filter_active(context jsonb, input jsonb)
RETURNS jsonb LANGUAGE plpgsql STABLE SET search_path = pg_catalog AS $$
DECLARE
    requested_bot text := context->>'bot_id';
    requested_ref text := input->>'query_ref_id';
    requested_space text := input->>'space_id';
    requested_binding jsonb := input->'binding';
    filtered jsonb;
BEGIN
    IF NOT EXISTS(
        SELECT 1 FROM __forest_module_schema__.vector_collection_health h
        JOIN __forest_module_schema__.vector_collection_bindings b
          ON b.bot_id=h.bot_id AND b.binding_revision=h.binding_revision
        WHERE h.bot_id=requested_bot AND h.state='READY'
          AND b.space_id=requested_space
          AND b.binding_revision=(requested_binding->>'binding_revision')::bigint
          AND b.collection_id=(requested_binding->>'collection_id')::bigint
          AND b.collection_name=(requested_binding->>'collection_name')
    ) THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object('code','VECTOR_INDEX_BINDING_STALE',
                'message','Vector binding changed during search','status',503));
    END IF;
    IF NOT EXISTS(
        SELECT 1 FROM __forest_module_schema__.vector_index_refs
        WHERE bot_id=requested_bot AND ref_id=requested_ref
          AND space_id=requested_space AND state='ACTIVE' AND purpose='QUERY'
    ) THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object('code','VECTOR_INDEX_QUERY_REF_MISSING',
                'message','Query vector ref is unavailable','status',409));
    END IF;
    SELECT COALESCE(jsonb_agg(hit ORDER BY position),'[]'::jsonb) INTO filtered
    FROM jsonb_array_elements(input->'hits') WITH ORDINALITY AS h(hit,position)
    WHERE EXISTS(
        SELECT 1 FROM __forest_module_schema__.vector_index_refs r
        WHERE r.bot_id=requested_bot AND r.ref_id=hit->>'ref_id'
          AND r.space_id=requested_space AND r.state='ACTIVE'
          AND r.purpose='DOCUMENT'
    );
    RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',true,
        'value',jsonb_build_object('query_ref_id',requested_ref,
            'space_id',requested_space,'hits',filtered));
END;
$$;

-- Resolve the package-owned active physical name before startup observes Milvus.
-- A replacement Generation must follow the published binding, not recreate the
-- deterministic first-generation collection after a future rebuild switch.
CREATE FUNCTION vector_collection_current(context jsonb, input jsonb)
RETURNS jsonb LANGUAGE plpgsql STABLE SET search_path = pg_catalog AS $$
DECLARE
    requested_bot text := context->>'bot_id';
    binding __forest_module_schema__.vector_collection_bindings%ROWTYPE;
BEGIN
    SELECT * INTO binding FROM __forest_module_schema__.vector_collection_bindings
    WHERE bot_id=requested_bot ORDER BY binding_revision DESC LIMIT 1;
    RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',true,
        'value',jsonb_build_object('binding',CASE WHEN FOUND THEN
            jsonb_build_object(
                'binding_revision',binding.binding_revision,
                'collection_id',binding.collection_id,
                'collection_name',binding.collection_name)
            ELSE 'null'::jsonb END));
END;
$$;

-- A startup command decides collection reuse/creation from the package ledger.
-- The host supplies observed physical identity but cannot mutate the ledger.
CREATE FUNCTION vector_collection_plan(context jsonb, input jsonb)
RETURNS jsonb LANGUAGE plpgsql VOLATILE SET search_path = pg_catalog AS $$
DECLARE
    requested_bot text := context->>'bot_id';
    requested_token text := context->>'operation_id';
    requested_space text := input->>'space_id';
    requested_name text := input->>'collection_name';
    requested_dimensions integer;
    requested_metric text := input->>'metric';
    observed_exists boolean;
    observed_id bigint;
    binding __forest_module_schema__.vector_collection_bindings%ROWTYPE;
    claim __forest_module_schema__.vector_collection_start_claims%ROWTYPE;
    health __forest_module_schema__.vector_collection_health%ROWTYPE;
    selected_revision bigint;
    selected_id bigint;
    selected_state text;
    selected_reason text;
    selected_token text;
BEGIN
    IF requested_bot IS NULL OR requested_token IS NULL
       OR octet_length(requested_token) NOT BETWEEN 1 AND 256
       OR requested_space IS NULL OR octet_length(requested_space) NOT BETWEEN 1 AND 256
       OR requested_name IS NULL OR octet_length(requested_name) NOT BETWEEN 1 AND 256
       OR jsonb_typeof(input->'dimensions') IS DISTINCT FROM 'number'
       OR jsonb_typeof(input->'exists') IS DISTINCT FROM 'boolean'
       OR requested_metric IS DISTINCT FROM 'COSINE' THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object('code','VECTOR_INDEX_COLLECTION_INPUT_INVALID',
                'message','Collection observation is invalid','status',422));
    END IF;

    requested_dimensions := (input->>'dimensions')::integer;
    observed_exists := (input->>'exists')::boolean;
    IF requested_dimensions NOT BETWEEN 1 AND 4096
       OR (observed_exists AND jsonb_typeof(input->'collection_id') IS DISTINCT FROM 'number')
       OR (NOT observed_exists AND input->'collection_id'<>'null'::jsonb) THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object('code','VECTOR_INDEX_COLLECTION_INPUT_INVALID',
                'message','Collection observation is invalid','status',422));
    END IF;
    IF observed_exists THEN
        observed_id := (input->>'collection_id')::bigint;
        IF observed_id<=0 THEN
            RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
                'error',jsonb_build_object('code','VECTOR_INDEX_COLLECTION_INPUT_INVALID',
                    'message','Collection identity is invalid','status',422));
        END IF;
    END IF;
    PERFORM pg_advisory_xact_lock(hashtextextended(
        'vector-collection:'||requested_bot,0));
    SELECT * INTO binding FROM __forest_module_schema__.vector_collection_bindings
    WHERE bot_id=requested_bot ORDER BY binding_revision DESC LIMIT 1;
    IF FOUND THEN
        IF binding.space_id IS DISTINCT FROM requested_space
           OR binding.collection_name IS DISTINCT FROM requested_name
           OR binding.dimensions IS DISTINCT FROM requested_dimensions
           OR binding.metric IS DISTINCT FROM requested_metric THEN
            RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
                'error',jsonb_build_object('code','VECTOR_INDEX_SPACE_IMMUTABLE',
                    'message','Collection binding belongs to another vector space',
                    'status',409));
        END IF;
        selected_revision := binding.binding_revision;
        selected_id := binding.collection_id;
        SELECT * INTO health FROM __forest_module_schema__.vector_collection_health
        WHERE bot_id=requested_bot;
        IF NOT FOUND OR health.binding_revision IS DISTINCT FROM binding.binding_revision THEN
            selected_state := 'REBUILD_REQUIRED';
            selected_reason := 'collection health ledger is incomplete';
        ELSIF health.state='REBUILD_REQUIRED' THEN
            selected_state := 'REBUILD_REQUIRED';
            selected_reason := health.reason;
        ELSE
            selected_state := CASE WHEN observed_exists
                AND observed_id=binding.collection_id THEN 'READY'
                ELSE 'REBUILD_REQUIRED' END;
            selected_reason := CASE WHEN selected_state='READY' THEN NULL
                WHEN observed_exists THEN 'collection identity changed'
                ELSE 'collection missing with binding' END;
        END IF;
    ELSE
        IF requested_name IS DISTINCT FROM 'forest_memory_ref2_' || substr(
            encode(sha256(
                convert_to(requested_bot,'UTF8') || decode('00','hex') ||
                convert_to(requested_space,'UTF8')
            ),'hex'),1,32
        ) THEN
            RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
                'error',jsonb_build_object('code','VECTOR_INDEX_COLLECTION_INPUT_INVALID',
                    'message','First collection name differs from its Bot space',
                    'status',422));
        END IF;
        SELECT * INTO claim FROM __forest_module_schema__.vector_collection_start_claims
        WHERE bot_id=requested_bot FOR UPDATE;
        IF FOUND AND claim.lease_deadline>clock_timestamp() THEN
            IF claim.space_id IS DISTINCT FROM requested_space
               OR claim.collection_name IS DISTINCT FROM requested_name
               OR claim.dimensions IS DISTINCT FROM requested_dimensions
               OR claim.metric IS DISTINCT FROM requested_metric THEN
                RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
                    'error',jsonb_build_object('code','VECTOR_INDEX_SPACE_IMMUTABLE',
                        'message','Collection startup claim has another shape',
                        'status',409));
            END IF;
            IF claim.claim_token=requested_token THEN
                selected_state := CASE WHEN observed_exists THEN 'CONFIRM' ELSE 'CREATE' END;
                selected_token := requested_token;
            ELSE
                selected_state := 'WAIT';
                selected_reason := 'another Generation is creating the collection';
            END IF;
        ELSE
            DELETE FROM __forest_module_schema__.vector_collection_start_claims
            WHERE bot_id=requested_bot;
            IF observed_exists THEN
                selected_state := 'REBUILD_REQUIRED';
                selected_reason := 'unbound collection';
            ELSIF EXISTS(
                SELECT 1 FROM __forest_module_schema__.vector_index_refs
                WHERE bot_id=requested_bot AND state IN ('PENDING','ACTIVE')
            ) THEN
                selected_state := 'REBUILD_REQUIRED';
                selected_reason := 'collection missing with pending or active refs';
            ELSE
                INSERT INTO __forest_module_schema__.vector_collection_start_claims(
                    bot_id,space_id,collection_name,dimensions,metric,
                    claim_token,lease_deadline
                ) VALUES(
                    requested_bot,requested_space,requested_name,
                    requested_dimensions,requested_metric,requested_token,
                    clock_timestamp()+interval '60 seconds'
                );
                selected_state := 'CREATE';
                selected_token := requested_token;
            END IF;
        END IF;
    END IF;
    RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',true,
        'value',jsonb_build_object('state',selected_state,
            'reason',selected_reason,'claim_token',selected_token,
            'binding_revision',selected_revision,'collection_id',selected_id));
END;
$$;

CREATE FUNCTION vector_collection_confirm(context jsonb, input jsonb)
RETURNS jsonb LANGUAGE plpgsql VOLATILE SET search_path = pg_catalog AS $$
DECLARE
    requested_bot text := context->>'bot_id';
    requested_token text := input->>'claim_token';
    requested_space text := input->>'space_id';
    requested_name text := input->>'collection_name';
    requested_dimensions integer := (input->>'dimensions')::integer;
    requested_metric text := input->>'metric';
    requested_id bigint := (input->>'collection_id')::bigint;
    claim __forest_module_schema__.vector_collection_start_claims%ROWTYPE;
BEGIN
    IF requested_bot IS NULL OR requested_token IS NULL
       OR requested_space IS NULL OR requested_name IS NULL
       OR requested_dimensions NOT BETWEEN 1 AND 4096
       OR requested_metric IS DISTINCT FROM 'COSINE'
       OR requested_id IS NULL OR requested_id<=0 THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object('code','VECTOR_INDEX_COLLECTION_INPUT_INVALID',
                'message','Collection confirmation is invalid','status',422));
    END IF;
    IF requested_name IS DISTINCT FROM 'forest_memory_ref2_' || substr(
        encode(sha256(
            convert_to(requested_bot,'UTF8') || decode('00','hex') ||
            convert_to(requested_space,'UTF8')
        ),'hex'),1,32
    ) THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object('code','VECTOR_INDEX_COLLECTION_INPUT_INVALID',
                'message','Collection name differs from its Bot space','status',422));
    END IF;
    PERFORM pg_advisory_xact_lock(hashtextextended(
        'vector-collection:'||requested_bot,0));
    SELECT * INTO claim FROM __forest_module_schema__.vector_collection_start_claims
    WHERE bot_id=requested_bot FOR UPDATE;
    IF NOT FOUND OR claim.claim_token IS DISTINCT FROM requested_token
       OR claim.lease_deadline<=clock_timestamp()
       OR claim.space_id IS DISTINCT FROM requested_space
       OR claim.collection_name IS DISTINCT FROM requested_name
       OR claim.dimensions IS DISTINCT FROM requested_dimensions
       OR claim.metric IS DISTINCT FROM requested_metric THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object('code','VECTOR_INDEX_COLLECTION_CLAIM_STALE',
                'message','Collection startup claim is no longer current','status',409));
    END IF;
    IF EXISTS(
        SELECT 1 FROM __forest_module_schema__.vector_index_refs
        WHERE bot_id=requested_bot AND state IN ('PENDING','ACTIVE')
    ) OR EXISTS(
        SELECT 1 FROM __forest_module_schema__.vector_collection_bindings
        WHERE bot_id=requested_bot
    ) THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object('code','VECTOR_INDEX_COLLECTION_CLAIM_STALE',
                'message','Collection ledger changed during startup','status',409));
    END IF;
    INSERT INTO __forest_module_schema__.vector_collection_bindings(
        bot_id,binding_revision,space_id,collection_name,collection_id,
        dimensions,metric
    ) VALUES(
        requested_bot,1,requested_space,requested_name,requested_id,
        requested_dimensions,requested_metric
    );
    INSERT INTO __forest_module_schema__.vector_collection_health(
        bot_id,binding_revision,state
    ) VALUES(requested_bot,1,'READY');
    DELETE FROM __forest_module_schema__.vector_collection_start_claims
    WHERE bot_id=requested_bot;
    RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',true,
        'value',jsonb_build_object('state','READY','reason',NULL,'claim_token',NULL,
            'binding_revision',1,'collection_id',requested_id));
END;
$$;

CREATE FUNCTION vector_collection_quarantine(context jsonb, input jsonb)
RETURNS jsonb LANGUAGE plpgsql VOLATILE SET search_path = pg_catalog AS $$
DECLARE
    requested_bot text := context->>'bot_id';
    requested_revision bigint := (input->>'binding_revision')::bigint;
    requested_id bigint := (input->>'collection_id')::bigint;
    requested_reason text := input->>'reason';
    binding __forest_module_schema__.vector_collection_bindings%ROWTYPE;
BEGIN
    IF requested_bot IS NULL OR requested_revision IS NULL
       OR requested_id IS NULL OR requested_revision<=0 OR requested_id<=0
       OR requested_reason IS NULL OR requested_reason NOT IN (
           'collection missing','collection identity changed',
           'collection layout changed','row mismatch'
       ) THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object('code','VECTOR_INDEX_COLLECTION_INPUT_INVALID',
                'message','Collection quarantine request is invalid','status',422));
    END IF;
    PERFORM pg_advisory_xact_lock(hashtextextended(
        'vector-collection:'||requested_bot,0));
    SELECT * INTO binding FROM __forest_module_schema__.vector_collection_bindings
    WHERE bot_id=requested_bot ORDER BY binding_revision DESC LIMIT 1;
    IF NOT FOUND OR binding.binding_revision IS DISTINCT FROM requested_revision
       OR binding.collection_id IS DISTINCT FROM requested_id THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object('code','VECTOR_INDEX_COLLECTION_BINDING_STALE',
                'message','Collection binding changed before quarantine','status',409));
    END IF;
    INSERT INTO __forest_module_schema__.vector_collection_health(
        bot_id,binding_revision,state,reason
    ) VALUES(requested_bot,requested_revision,'REBUILD_REQUIRED',requested_reason)
    ON CONFLICT (bot_id) DO UPDATE
    SET state='REBUILD_REQUIRED',reason=EXCLUDED.reason,
        observed_at=clock_timestamp()
    WHERE vector_collection_health.binding_revision=requested_revision
      AND vector_collection_health.state='READY';
    RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',true,
        'value',jsonb_build_object('state','REBUILD_REQUIRED',
            'binding_revision',requested_revision,'collection_id',requested_id));
END;
$$;


-- The package owns a resumable audit cursor. The frozen mutation waterline
-- excludes refs published after the cycle began; a later cycle catches them.
CREATE INDEX vector_index_refs_active_audit_idx
    ON vector_index_refs(bot_id,ref_id) WHERE state='ACTIVE';
CREATE UNIQUE INDEX vector_ref_changes_active_ref_idx
    ON vector_ref_changes(bot_id,ref_id) WHERE state='ACTIVE';

CREATE TABLE vector_audit_work (
    bot_id text PRIMARY KEY REFERENCES forest_kernel.bot_identity(bot_id),
    space_id text NOT NULL CHECK (octet_length(space_id) BETWEEN 1 AND 256),
    binding_revision bigint NOT NULL CHECK (binding_revision > 0),
    collection_id bigint NOT NULL CHECK (collection_id > 0),
    phase text NOT NULL DEFAULT 'REF' CHECK (phase IN ('REF','PHYSICAL')),
    after_ref text CHECK (octet_length(after_ref) BETWEEN 1 AND 256),
    cycle_seq bigint CHECK (cycle_seq >= 0),
    next_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    claim_token text CHECK (octet_length(claim_token) BETWEEN 1 AND 256),
    lease_deadline timestamptz,
    attempt_count integer NOT NULL DEFAULT 0 CHECK (attempt_count >= 0),
    claim_payload jsonb,
    last_checked_seq bigint CHECK (last_checked_seq >= 0),
    last_checked_at timestamptz,
    CHECK ((claim_token IS NULL) = (lease_deadline IS NULL)),
    CHECK ((claim_token IS NULL) = (claim_payload IS NULL))
);
CREATE TABLE vector_audit_settlements (
    bot_id text NOT NULL REFERENCES forest_kernel.bot_identity(bot_id),
    claim_token text NOT NULL CHECK (octet_length(claim_token) BETWEEN 1 AND 256),
    input_payload jsonb NOT NULL,
    result_payload jsonb NOT NULL,
    settled_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (bot_id,claim_token)
);
CREATE INDEX vector_audit_settlements_gc_idx ON vector_audit_settlements(
    bot_id,settled_at,claim_token
);
CREATE FUNCTION guard_vector_audit_settlement() RETURNS trigger
LANGUAGE plpgsql SET search_path = pg_catalog AS $$
BEGIN
    IF TG_OP='DELETE'
       AND OLD.settled_at<clock_timestamp()-interval '48 hours' THEN
        RETURN OLD;
    END IF;
    RAISE EXCEPTION 'Vector audit settlement is immutable until expiry'
        USING ERRCODE='55000';
END;
$$;
CREATE TRIGGER vector_audit_settlement_guard
    BEFORE UPDATE OR DELETE ON vector_audit_settlements
    FOR EACH ROW EXECUTE FUNCTION guard_vector_audit_settlement();

CREATE FUNCTION vector_claim_ref_audit(context jsonb, input jsonb)
RETURNS jsonb LANGUAGE plpgsql VOLATILE SET search_path = pg_catalog AS $$
DECLARE
    requested_bot text := context->>'bot_id';
    requested_token text := input->>'claim_token';
    requested_lease integer := (input->>'lease_seconds')::integer;
    binding __forest_module_schema__.vector_collection_bindings%ROWTYPE;
    work __forest_module_schema__.vector_audit_work%ROWTYPE;
    selected_refs jsonb;
    selected_seq bigint;
    work_key text;
    next_attempt integer;
    next_payload jsonb;
BEGIN
    IF input->>'protocol' IS DISTINCT FROM 'forest.owned-work-claim-request/1'
       OR requested_bot IS NULL OR requested_token IS NULL
       OR octet_length(requested_token) NOT BETWEEN 1 AND 256
       OR requested_lease IS NULL OR requested_lease NOT BETWEEN 3 AND 86400 THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object('code','VECTOR_AUDIT_CLAIM_INVALID',
                'message','Vector audit claim is invalid','status',422));
    END IF;
    DELETE FROM __forest_module_schema__.vector_audit_settlements receipt
    WHERE (receipt.bot_id,receipt.claim_token) IN (
        SELECT old.bot_id,old.claim_token
        FROM __forest_module_schema__.vector_audit_settlements old
        WHERE old.bot_id=requested_bot
          AND old.settled_at<clock_timestamp()-interval '48 hours'
        ORDER BY old.settled_at,old.claim_token
        LIMIT 128 FOR UPDATE SKIP LOCKED
    );
    SELECT b.* INTO binding
    FROM __forest_module_schema__.vector_collection_health h
    JOIN __forest_module_schema__.vector_collection_bindings b
      ON b.bot_id=h.bot_id AND b.binding_revision=h.binding_revision
    WHERE h.bot_id=requested_bot AND h.state='READY';
    IF NOT FOUND THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1',
                                  'ok',true,'value',NULL);
    END IF;
    INSERT INTO __forest_module_schema__.vector_audit_work(
        bot_id,space_id,binding_revision,collection_id
    ) VALUES(
        requested_bot,binding.space_id,binding.binding_revision,binding.collection_id
    ) ON CONFLICT (bot_id) DO NOTHING;
    SELECT * INTO work FROM __forest_module_schema__.vector_audit_work
    WHERE bot_id=requested_bot FOR UPDATE;
    IF work.binding_revision<>binding.binding_revision
       OR work.collection_id<>binding.collection_id THEN
        UPDATE __forest_module_schema__.vector_audit_work
        SET space_id=binding.space_id,
            binding_revision=binding.binding_revision,
            collection_id=binding.collection_id,
            phase='REF',after_ref=NULL,cycle_seq=NULL,
            next_at=clock_timestamp(),claim_token=NULL,
            lease_deadline=NULL,claim_payload=NULL
        WHERE bot_id=requested_bot;
        SELECT * INTO work FROM __forest_module_schema__.vector_audit_work
        WHERE bot_id=requested_bot;
    END IF;
    IF work.claim_token=requested_token
       AND work.lease_deadline>clock_timestamp() THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1',
                                  'ok',true,'value',work.claim_payload);
    END IF;
    IF work.next_at>clock_timestamp()
       OR (work.lease_deadline IS NOT NULL
           AND work.lease_deadline>clock_timestamp()) THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1',
                                  'ok',true,'value',NULL);
    END IF;
    selected_seq := work.cycle_seq;
    IF selected_seq IS NULL THEN
        SELECT current_seq INTO selected_seq
        FROM __forest_module_schema__.vector_ref_clocks
        WHERE bot_id=requested_bot;
        selected_seq := COALESCE(selected_seq,0);
    END IF;
    selected_refs := '[]'::jsonb;
    IF work.phase='REF' THEN
        SELECT COALESCE(jsonb_agg(ref_id ORDER BY ref_id),'[]'::jsonb)
          INTO selected_refs
        FROM (
            SELECT r.ref_id
            FROM __forest_module_schema__.vector_index_refs r
            LEFT JOIN __forest_module_schema__.vector_ref_changes c
              ON c.bot_id=r.bot_id AND c.ref_id=r.ref_id
             AND c.state='ACTIVE'
            WHERE r.bot_id=requested_bot AND r.state='ACTIVE'
              AND (c.mutation_seq<=selected_seq OR c.mutation_seq IS NULL)
              AND (work.after_ref IS NULL OR r.ref_id>work.after_ref)
            ORDER BY r.ref_id LIMIT 64
        ) page;
        IF jsonb_array_length(selected_refs)=0 THEN
            work.phase := 'PHYSICAL';
            work.after_ref := NULL;
        END IF;
    END IF;
    next_attempt := work.attempt_count+1;
    work_key := 'vector-audit:' ||
        encode(sha256(convert_to(requested_bot||'/'||requested_token,'UTF8')),'hex');
    next_payload := jsonb_build_object(
        'protocol','forest.owned-work-claim/1',
        'work_id',work_key,'claim_token',requested_token,
        'attempt',next_attempt,'operation_id',work_key,
        'causal_root_kind','vector.audit','causal_root_id',work_key,
        'payload',jsonb_build_object(
            'space_id',binding.space_id,'phase',work.phase,
            'claim_token',requested_token,
            'after_ref',work.after_ref,'limit',64,'cycle_seq',selected_seq,
            'binding_revision',binding.binding_revision,
            'collection_id',binding.collection_id,
            'collection_name',binding.collection_name,'refs',selected_refs));
    UPDATE __forest_module_schema__.vector_audit_work
    SET phase=work.phase,after_ref=work.after_ref,cycle_seq=selected_seq,
        claim_token=requested_token,
        lease_deadline=clock_timestamp()+(requested_lease*interval '1 second'),
        attempt_count=next_attempt,claim_payload=next_payload
    WHERE bot_id=requested_bot;
    RETURN jsonb_build_object('protocol','forest.domain-result/1',
                              'ok',true,'value',next_payload);
END;
$$;

-- Compare physical observations against current SQL authority only after
-- freezing the cycle waterline. A late activation is deferred, not condemned.
CREATE FUNCTION vector_adjudicate_ref_audit(context jsonb, input jsonb)
RETURNS jsonb LANGUAGE plpgsql VOLATILE SET search_path = pg_catalog AS $$
DECLARE
    requested_bot text := context->>'bot_id';
    requested_space text := input->>'space_id';
    requested_revision bigint := (input->>'binding_revision')::bigint;
    requested_id bigint := (input->>'collection_id')::bigint;
    requested_seq bigint := (input->>'cycle_seq')::bigint;
    requested_token text := input->>'claim_token';
    observations jsonb := input->'observations';
    observation jsonb;
    physical jsonb;
    requested_ref text;
    current_ref __forest_module_schema__.vector_index_refs%ROWTYPE;
    published_seq bigint;
    health __forest_module_schema__.vector_collection_health%ROWTYPE;
    work __forest_module_schema__.vector_audit_work%ROWTYPE;
    mismatch_ref text;
BEGIN
    IF requested_bot IS NULL OR requested_space IS NULL
       OR requested_token IS NULL
       OR octet_length(requested_token) NOT BETWEEN 1 AND 256
       OR requested_revision IS NULL OR requested_revision<=0
       OR requested_id IS NULL OR requested_id<=0
       OR requested_seq IS NULL OR requested_seq<0
       OR jsonb_typeof(observations) IS DISTINCT FROM 'array'
       OR jsonb_array_length(observations) NOT BETWEEN 1 AND 64 THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object('code','VECTOR_AUDIT_INPUT_INVALID',
                'message','Vector audit observations are invalid','status',422));
    END IF;
    PERFORM pg_advisory_xact_lock(hashtextextended(
        'vector-collection:'||requested_bot,0));
    SELECT * INTO work FROM __forest_module_schema__.vector_audit_work
    WHERE bot_id=requested_bot FOR SHARE;
    IF NOT FOUND OR work.claim_token IS DISTINCT FROM requested_token
       OR work.lease_deadline<=clock_timestamp()
       OR work.cycle_seq IS DISTINCT FROM requested_seq
       OR work.space_id IS DISTINCT FROM requested_space
       OR work.binding_revision IS DISTINCT FROM requested_revision
       OR work.collection_id IS DISTINCT FROM requested_id THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object('code','VECTOR_AUDIT_CLAIM_STALE',
                'message','Vector audit lease is no longer current','status',409));
    END IF;
    SELECT * INTO health FROM __forest_module_schema__.vector_collection_health
    WHERE bot_id=requested_bot FOR UPDATE;
    IF NOT FOUND OR health.binding_revision<>requested_revision
       OR NOT EXISTS(
           SELECT 1 FROM __forest_module_schema__.vector_collection_bindings b
           WHERE b.bot_id=requested_bot
             AND b.binding_revision=requested_revision
             AND b.collection_id=requested_id AND b.space_id=requested_space
       ) THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object('code','VECTOR_AUDIT_BINDING_STALE',
                'message','Vector audit binding changed','status',409));
    END IF;
    IF health.state='REBUILD_REQUIRED' THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',true,
            'value',jsonb_build_object('state','REBUILD_REQUIRED','ref_id',NULL));
    END IF;
    FOR observation IN SELECT value FROM jsonb_array_elements(observations)
    LOOP
        requested_ref := observation->>'ref_id';
        physical := observation->'physical';
        IF requested_ref IS NULL
           OR octet_length(requested_ref) NOT BETWEEN 1 AND 256
           OR jsonb_typeof(physical) NOT IN ('object','null')
           OR (jsonb_typeof(physical)='object'
               AND physical->>'ref_id' IS DISTINCT FROM requested_ref) THEN
            RETURN jsonb_build_object('protocol','forest.domain-result/1',
                'ok',false,'error',jsonb_build_object(
                    'code','VECTOR_AUDIT_INPUT_INVALID',
                    'message','Vector audit row is invalid','status',422));
        END IF;
        SELECT * INTO current_ref
        FROM __forest_module_schema__.vector_index_refs
        WHERE bot_id=requested_bot AND ref_id=requested_ref FOR UPDATE;
        IF NOT FOUND THEN
            mismatch_ref := requested_ref;
            EXIT;
        END IF;
        IF current_ref.state<>'ACTIVE' THEN
            CONTINUE;
        END IF;
        SELECT c.mutation_seq INTO published_seq
        FROM __forest_module_schema__.vector_ref_changes c
        WHERE c.bot_id=requested_bot AND c.ref_id=requested_ref
          AND c.state='ACTIVE';
        IF published_seq IS NULL THEN
            mismatch_ref := requested_ref;
            EXIT;
        END IF;
        IF published_seq>requested_seq THEN
            CONTINUE;
        END IF;
        IF jsonb_typeof(physical) IS DISTINCT FROM 'object'
           OR physical->>'bot_id' IS DISTINCT FROM requested_bot
           OR physical->>'space_id' IS DISTINCT FROM current_ref.space_id
           OR physical->>'purpose' IS DISTINCT FROM current_ref.purpose
           OR physical->>'input_digest' IS DISTINCT FROM current_ref.input_digest
           OR physical->>'physical_digest' IS DISTINCT FROM current_ref.physical_digest THEN
            mismatch_ref := requested_ref;
            EXIT;
        END IF;
    END LOOP;
    IF mismatch_ref IS NOT NULL THEN
        UPDATE __forest_module_schema__.vector_collection_health
        SET state='REBUILD_REQUIRED',reason='row mismatch',
            observed_at=clock_timestamp()
        WHERE bot_id=requested_bot AND binding_revision=requested_revision
          AND state='READY';
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',true,
            'value',jsonb_build_object('state','REBUILD_REQUIRED',
                                        'ref_id',mismatch_ref));
    END IF;
    RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',true,
        'value',jsonb_build_object('state','READY','ref_id',NULL));
END;
$$;

CREATE FUNCTION vector_settle_ref_audit(context jsonb, input jsonb)
RETURNS jsonb LANGUAGE plpgsql VOLATILE SET search_path = pg_catalog AS $$
DECLARE
    requested_bot text := context->>'bot_id';
    requested_token text := input->>'claim_token';
    requested_work text := input->>'work_id';
    requested_attempt integer := (input->>'attempt')::integer;
    outcome jsonb := input->'outcome';
    result_value jsonb := outcome->'result';
    work __forest_module_schema__.vector_audit_work%ROWTYPE;
    existing_input jsonb;
    existing_result jsonb;
    expected_work text;
    expected_next text;
    expected_done boolean;
    next_at_value timestamptz;
    receipt jsonb;
BEGIN
    IF input->>'protocol' IS DISTINCT FROM 'forest.owned-work-settle-request/1'
       OR requested_bot IS NULL OR requested_token IS NULL
       OR octet_length(requested_token) NOT BETWEEN 1 AND 256
       OR requested_work IS NULL OR requested_attempt IS NULL
       OR outcome->>'status' IS NULL
       OR outcome->>'status' NOT IN ('SUCCEEDED','FAILED') THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object('code','VECTOR_AUDIT_SETTLE_INVALID',
                'message','Vector audit settlement is invalid','status',422));
    END IF;
    expected_work := 'vector-audit:' ||
        encode(sha256(convert_to(requested_bot||'/'||requested_token,'UTF8')),'hex');
    IF requested_work IS DISTINCT FROM expected_work THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object('code','VECTOR_AUDIT_SETTLE_INVALID',
                'message','Vector audit work identity differs','status',422));
    END IF;
    PERFORM pg_advisory_xact_lock(hashtextextended(
        'vector-audit-settle:'||requested_bot||':'||requested_token,0));
    SELECT input_payload,result_payload INTO existing_input,existing_result
    FROM __forest_module_schema__.vector_audit_settlements
    WHERE bot_id=requested_bot AND claim_token=requested_token;
    IF FOUND THEN
        IF existing_input=input THEN RETURN existing_result; END IF;
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object('code','VECTOR_AUDIT_SETTLE_CONFLICT',
                'message','Vector audit claim was settled differently','status',409));
    END IF;
    SELECT * INTO work FROM __forest_module_schema__.vector_audit_work
    WHERE bot_id=requested_bot FOR UPDATE;
    IF NOT FOUND OR work.claim_token IS DISTINCT FROM requested_token
       OR work.attempt_count<>requested_attempt
       OR work.lease_deadline<=clock_timestamp()
       OR work.claim_payload->>'work_id' IS DISTINCT FROM requested_work THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object('code','VECTOR_AUDIT_CLAIM_STALE',
                'message','Vector audit lease is no longer current','status',409));
    END IF;
    IF outcome->>'status'='SUCCEEDED' THEN
        IF result_value->>'space_id' IS DISTINCT FROM work.space_id
           OR result_value->>'phase' IS DISTINCT FROM work.phase
           OR (result_value->>'binding_revision')::bigint<>work.binding_revision
           OR (result_value->>'collection_id')::bigint<>work.collection_id
           OR jsonb_typeof(result_value->'done') IS DISTINCT FROM 'boolean'
           OR jsonb_typeof(result_value->'quarantined') IS DISTINCT FROM 'boolean' THEN
            RETURN jsonb_build_object('protocol','forest.domain-result/1',
                'ok',false,'error',jsonb_build_object(
                    'code','VECTOR_AUDIT_SETTLE_INVALID',
                    'message','Vector audit result differs from its claim',
                    'status',422));
        END IF;
        IF work.phase='REF' THEN
            SELECT max(value) INTO expected_next
            FROM jsonb_array_elements_text(work.claim_payload#>'{payload,refs}');
            expected_done :=
                jsonb_array_length(work.claim_payload#>'{payload,refs}')<64;
            IF result_value->>'next_ref' IS DISTINCT FROM expected_next
               OR (result_value->>'done')::boolean IS DISTINCT FROM expected_done THEN
                RETURN jsonb_build_object('protocol','forest.domain-result/1',
                    'ok',false,'error',jsonb_build_object(
                        'code','VECTOR_AUDIT_SETTLE_INVALID',
                        'message','Vector audit ref cursor differs','status',422));
            END IF;
        ELSIF (result_value->>'done')::boolean=false
           AND (result_value->>'next_ref' IS NULL
                OR (work.after_ref IS NOT NULL
                    AND result_value->>'next_ref'<=work.after_ref)) THEN
            RETURN jsonb_build_object('protocol','forest.domain-result/1',
                'ok',false,'error',jsonb_build_object(
                    'code','VECTOR_AUDIT_SETTLE_INVALID',
                    'message','Vector audit physical cursor did not advance',
                    'status',422));
        END IF;
        IF (result_value->>'quarantined')::boolean
           AND NOT EXISTS(
               SELECT 1 FROM __forest_module_schema__.vector_collection_health h
               WHERE h.bot_id=requested_bot
                 AND h.binding_revision=work.binding_revision
                 AND h.state='REBUILD_REQUIRED'
           ) THEN
            RETURN jsonb_build_object('protocol','forest.domain-result/1',
                'ok',false,'error',jsonb_build_object(
                    'code','VECTOR_AUDIT_SETTLE_INVALID',
                    'message','Audit quarantine has no authority fact',
                    'status',422));
        END IF;
        IF (result_value->>'quarantined')::boolean THEN
            next_at_value := clock_timestamp()+interval '1 hour';
            UPDATE __forest_module_schema__.vector_audit_work
            SET phase='REF',after_ref=NULL,cycle_seq=NULL,
                next_at=next_at_value,claim_token=NULL,
                lease_deadline=NULL,claim_payload=NULL
            WHERE bot_id=requested_bot;
        ELSIF work.phase='REF' THEN
            UPDATE __forest_module_schema__.vector_audit_work
            SET phase=CASE WHEN expected_done THEN 'PHYSICAL' ELSE 'REF' END,
                after_ref=CASE WHEN expected_done THEN NULL ELSE expected_next END,
                next_at=clock_timestamp(),claim_token=NULL,
                lease_deadline=NULL,claim_payload=NULL
            WHERE bot_id=requested_bot;
        ELSIF (result_value->>'done')::boolean THEN
            next_at_value := clock_timestamp()+
                CASE WHEN EXISTS(
                    SELECT 1 FROM __forest_module_schema__.vector_ref_clocks
                    WHERE bot_id=requested_bot AND current_seq>work.cycle_seq
                ) THEN interval '1 minute' ELSE interval '1 hour' END;
            UPDATE __forest_module_schema__.vector_audit_work
            SET phase='REF',after_ref=NULL,cycle_seq=NULL,
                next_at=next_at_value,claim_token=NULL,
                lease_deadline=NULL,claim_payload=NULL,
                last_checked_seq=work.cycle_seq,last_checked_at=clock_timestamp()
            WHERE bot_id=requested_bot;
        ELSE
            UPDATE __forest_module_schema__.vector_audit_work
            SET after_ref=result_value->>'next_ref',
                next_at=clock_timestamp(),claim_token=NULL,
                lease_deadline=NULL,claim_payload=NULL
            WHERE bot_id=requested_bot;
        END IF;
    ELSE
        UPDATE __forest_module_schema__.vector_audit_work
        SET next_at=clock_timestamp()+interval '30 seconds',
            claim_token=NULL,lease_deadline=NULL,claim_payload=NULL
        WHERE bot_id=requested_bot;
    END IF;
    receipt := jsonb_build_object('protocol','forest.domain-result/1','ok',true,
        'value',jsonb_build_object(
            'protocol','forest.owned-work-settled/1',
            'work_id',requested_work));
    INSERT INTO __forest_module_schema__.vector_audit_settlements(
        bot_id,claim_token,input_payload,result_payload
    ) VALUES(requested_bot,requested_token,input,receipt);
    RETURN receipt;
END;
$$;


-- A rebuild request is durable package authority. Candidate revisions are
-- never reused: an expired unconfirmed worker can only touch its old name.
CREATE TABLE vector_rebuild_work (
    bot_id text NOT NULL REFERENCES forest_kernel.bot_identity(bot_id),
    request_id text NOT NULL CHECK (octet_length(request_id) BETWEEN 1 AND 256),
    input_digest text NOT NULL CHECK (input_digest ~ '^sha256:[a-f0-9]{64}$'),
    requested_by text NOT NULL CHECK (octet_length(requested_by) BETWEEN 1 AND 256),
    space_id text NOT NULL CHECK (octet_length(space_id) BETWEEN 1 AND 256),
    base_revision bigint NOT NULL CHECK (base_revision>0),
    dimensions integer NOT NULL CHECK (dimensions BETWEEN 1 AND 4096),
    state text NOT NULL DEFAULT 'QUEUED' CHECK
        (state IN ('QUEUED','CLAIMED','CANDIDATE_READY','REPLAY_CLAIMED','AUDIT_READY','AUDIT_CLAIMED','AUDITED','PUBLISHING','PUBLISHED','FAILED')),
    attempt_count integer NOT NULL DEFAULT 0 CHECK (attempt_count>=0),
    candidate_revision bigint,
    candidate_name text,
    candidate_id bigint CHECK (candidate_id>0),
    replay_waterline bigint CHECK (replay_waterline>=0),
    replay_cursor bigint NOT NULL DEFAULT 0 CHECK (replay_cursor>=0),
    replay_attempt_count bigint NOT NULL DEFAULT 0 CHECK (replay_attempt_count>=0),
    audit_seq bigint CHECK (audit_seq>=0),
    audit_after_ref text CHECK (octet_length(audit_after_ref) BETWEEN 1 AND 256),
    audit_claim_count bigint NOT NULL DEFAULT 0 CHECK (audit_claim_count>=0),
    audit_failure_count integer NOT NULL DEFAULT 0 CHECK (audit_failure_count>=0),
    publish_attempt_count bigint NOT NULL DEFAULT 0 CHECK (publish_attempt_count>=0),
    audited_at timestamptz,
    published_at timestamptz,
    published_seq bigint CHECK (published_seq>=0),
    claim_token text,
    lease_deadline timestamptz,
    claim_payload jsonb,
    error_code text,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY(bot_id,request_id),
    CHECK ((claim_token IS NULL)=(lease_deadline IS NULL)),
    CHECK ((claim_token IS NULL)=(claim_payload IS NULL)),
    CHECK ((candidate_revision IS NULL)=(candidate_name IS NULL)),
    CHECK (candidate_id IS NULL OR candidate_revision IS NOT NULL),
    CHECK (state NOT IN ('CANDIDATE_READY','REPLAY_CLAIMED',
                         'AUDIT_READY','AUDIT_CLAIMED','AUDITED',
                         'PUBLISHING','PUBLISHED') OR
           (candidate_id IS NOT NULL AND replay_waterline IS NOT NULL)),
    CHECK (state NOT IN ('AUDIT_READY','AUDIT_CLAIMED','AUDITED',
                         'PUBLISHING','PUBLISHED') OR audit_seq IS NOT NULL),
    CHECK ((state IN ('AUDITED','PUBLISHING','PUBLISHED'))=
           (audited_at IS NOT NULL)),
    CHECK ((state='PUBLISHED')=(published_at IS NOT NULL)),
    CHECK ((published_at IS NULL)=(published_seq IS NULL))
);
CREATE UNIQUE INDEX vector_rebuild_one_active_idx ON vector_rebuild_work(bot_id)
    WHERE state IN ('QUEUED','CLAIMED','CANDIDATE_READY',
                    'REPLAY_CLAIMED','AUDIT_READY','AUDIT_CLAIMED','AUDITED',
                    'PUBLISHING');
CREATE TABLE vector_rebuild_attempts (
    bot_id text NOT NULL REFERENCES forest_kernel.bot_identity(bot_id),
    candidate_revision bigint NOT NULL CHECK (candidate_revision>1),
    request_id text NOT NULL,
    attempt integer NOT NULL CHECK (attempt>0),
    collection_name text NOT NULL CHECK (octet_length(collection_name) BETWEEN 1 AND 256),
    collection_id bigint CHECK (collection_id>0),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY(bot_id,candidate_revision),
    UNIQUE(bot_id,request_id,attempt),
    FOREIGN KEY(bot_id,request_id) REFERENCES vector_rebuild_work(bot_id,request_id)
);
CREATE TABLE vector_rebuild_settlements (
    bot_id text NOT NULL REFERENCES forest_kernel.bot_identity(bot_id),
    claim_token text NOT NULL CHECK (octet_length(claim_token) BETWEEN 1 AND 256),
    input_payload jsonb NOT NULL,
    result_payload jsonb NOT NULL,
    settled_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY(bot_id,claim_token)
);

CREATE INDEX vector_rebuild_settlements_gc_idx ON vector_rebuild_settlements(
    bot_id,settled_at,claim_token
);
CREATE FUNCTION guard_vector_rebuild_settlement() RETURNS trigger
LANGUAGE plpgsql SET search_path=pg_catalog AS $$
BEGIN
    IF TG_OP='DELETE' AND OLD.settled_at<clock_timestamp()-interval '48 hours' THEN
        RETURN OLD;
    END IF;
    RAISE EXCEPTION 'Vector rebuild settlement is immutable until expiry'
        USING ERRCODE='55000';
END;
$$;
CREATE TRIGGER vector_rebuild_settlement_guard
    BEFORE UPDATE OR DELETE ON vector_rebuild_settlements
    FOR EACH ROW EXECUTE FUNCTION guard_vector_rebuild_settlement();

CREATE FUNCTION vector_enqueue_rebuild(context jsonb, input jsonb)
RETURNS jsonb LANGUAGE plpgsql VOLATILE SET search_path=pg_catalog AS $$
DECLARE
    requested_bot text := context->>'bot_id';
    requested_id text := context->>'operation_id';
    requested_actor text := input->>'requested_by';
    requested_digest text := 'sha256:'||
        encode(sha256(convert_to(input::text,'UTF8')),'hex');
    binding __forest_module_schema__.vector_collection_bindings%ROWTYPE;
    health __forest_module_schema__.vector_collection_health%ROWTYPE;
    work __forest_module_schema__.vector_rebuild_work%ROWTYPE;
BEGIN
    IF requested_bot IS NULL OR requested_id IS NULL
       OR octet_length(requested_id) NOT BETWEEN 1 AND 256
       OR requested_actor IS NULL
       OR octet_length(requested_actor) NOT BETWEEN 1 AND 256 THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object('code','VECTOR_REBUILD_INPUT_INVALID',
                'message','Vector rebuild request is invalid','status',422));
    END IF;
    PERFORM pg_advisory_xact_lock(hashtextextended('vector-rebuild:'||requested_bot,0));
    SELECT * INTO work FROM __forest_module_schema__.vector_rebuild_work
    WHERE bot_id=requested_bot AND request_id=requested_id;
    IF FOUND THEN
        IF work.input_digest IS DISTINCT FROM requested_digest THEN
            RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
                'error',jsonb_build_object('code','VECTOR_REBUILD_REQUEST_CONFLICT',
                    'message','Rebuild operation was reused','status',409));
        END IF;
    ELSE
        SELECT * INTO health FROM __forest_module_schema__.vector_collection_health
        WHERE bot_id=requested_bot;
        SELECT * INTO binding FROM __forest_module_schema__.vector_collection_bindings
        WHERE bot_id=requested_bot AND binding_revision=health.binding_revision;
        IF NOT FOUND THEN
            RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
                'error',jsonb_build_object('code','VECTOR_REBUILD_BINDING_MISSING',
                    'message','No published vector binding exists','status',409));
        END IF;
        IF EXISTS(SELECT 1 FROM __forest_module_schema__.vector_rebuild_work
            WHERE bot_id=requested_bot
              AND state IN ('QUEUED','CLAIMED','CANDIDATE_READY')) THEN
            RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
                'error',jsonb_build_object('code','VECTOR_REBUILD_IN_PROGRESS',
                    'message','Another rebuild is in progress','status',409));
        END IF;
        INSERT INTO __forest_module_schema__.vector_rebuild_work(
            bot_id,request_id,input_digest,requested_by,space_id,
            base_revision,dimensions
        ) VALUES(requested_bot,requested_id,requested_digest,requested_actor,
                 binding.space_id,binding.binding_revision,binding.dimensions)
        RETURNING * INTO work;
    END IF;
    RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',true,
        'value',jsonb_build_object('request_id',work.request_id,
            'state',work.state,'space_id',work.space_id));
END;
$$;

CREATE FUNCTION vector_query_rebuild(context jsonb, input jsonb)
RETURNS jsonb LANGUAGE plpgsql STABLE SET search_path=pg_catalog AS $$
DECLARE
    work __forest_module_schema__.vector_rebuild_work%ROWTYPE;
BEGIN
    SELECT * INTO work FROM __forest_module_schema__.vector_rebuild_work
    WHERE bot_id=context->>'bot_id' AND request_id=input->>'request_id'
      AND requested_by=input->>'requested_by';
    IF NOT FOUND THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object('code','VECTOR_REBUILD_NOT_FOUND',
                'message','Rebuild request is unavailable','status',404));
    END IF;
    RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',true,
        'value',jsonb_build_object('request_id',work.request_id,
            'state',work.state,'space_id',work.space_id,
            'base_revision',work.base_revision,
            'candidate_revision',work.candidate_revision,
            'candidate_name',work.candidate_name,
            'candidate_id',work.candidate_id,
            'replay_waterline',work.replay_waterline,
            'replay_cursor',work.replay_cursor,
            'audit_seq',work.audit_seq,
            'audit_after_ref',work.audit_after_ref,
            'audited_at',work.audited_at,
            'published_at',work.published_at,
            'published_seq',work.published_seq,
            'attempt_count',work.attempt_count,'error_code',work.error_code));
END;
$$;


CREATE FUNCTION vector_claim_rebuild_create(context jsonb, input jsonb)
RETURNS jsonb LANGUAGE plpgsql VOLATILE SET search_path=pg_catalog AS $$
DECLARE
    requested_bot text := context->>'bot_id';
    requested_token text := input->>'claim_token';
    requested_lease integer := (input->>'lease_seconds')::integer;
    work __forest_module_schema__.vector_rebuild_work%ROWTYPE;
    next_revision bigint;
    base_name text;
    next_name text;
    next_payload jsonb;
    work_key text;
BEGIN
    IF input->>'protocol' IS DISTINCT FROM 'forest.owned-work-claim-request/1'
       OR requested_bot IS NULL OR requested_token IS NULL
       OR octet_length(requested_token) NOT BETWEEN 1 AND 256
       OR requested_lease IS NULL OR requested_lease NOT BETWEEN 3 AND 86400 THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object('code','VECTOR_REBUILD_CLAIM_INVALID',
                'message','Rebuild claim is invalid','status',422));
    END IF;
    PERFORM pg_advisory_xact_lock(hashtextextended('vector-rebuild:'||requested_bot,0));
    DELETE FROM __forest_module_schema__.vector_rebuild_settlements
    WHERE bot_id=requested_bot AND claim_token IN (
        SELECT claim_token FROM __forest_module_schema__.vector_rebuild_settlements
        WHERE bot_id=requested_bot
          AND settled_at<clock_timestamp()-interval '48 hours'
        ORDER BY settled_at,claim_token LIMIT 16
    );
    SELECT * INTO work FROM __forest_module_schema__.vector_rebuild_work
    WHERE bot_id=requested_bot AND state='CLAIMED'
    ORDER BY updated_at LIMIT 1 FOR UPDATE;
    IF FOUND AND work.claim_token=requested_token
       AND work.lease_deadline>clock_timestamp() THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1',
            'ok',true,'value',work.claim_payload);
    END IF;
    IF FOUND AND work.lease_deadline<=clock_timestamp() THEN
        UPDATE __forest_module_schema__.vector_rebuild_work
        SET state=CASE WHEN attempt_count>=5 THEN 'FAILED' ELSE 'QUEUED' END,
            claim_token=NULL,lease_deadline=NULL,claim_payload=NULL,
            error_code='VECTOR_REBUILD_LEASE_EXPIRED',
            updated_at=clock_timestamp()
        WHERE bot_id=requested_bot AND request_id=work.request_id;
    END IF;
    SELECT * INTO work FROM __forest_module_schema__.vector_rebuild_work
    WHERE bot_id=requested_bot AND state='QUEUED'
    ORDER BY created_at,request_id LIMIT 1 FOR UPDATE;
    IF NOT FOUND THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1',
            'ok',true,'value',NULL);
    END IF;
    IF NOT EXISTS(
        SELECT 1 FROM __forest_module_schema__.vector_collection_health h
        WHERE h.bot_id=requested_bot AND h.binding_revision=work.base_revision
    ) THEN
        UPDATE __forest_module_schema__.vector_rebuild_work
        SET state='FAILED',error_code='VECTOR_REBUILD_BASE_CHANGED',
            updated_at=clock_timestamp()
        WHERE bot_id=requested_bot AND request_id=work.request_id;
        RETURN jsonb_build_object('protocol','forest.domain-result/1',
            'ok',true,'value',NULL);
    END IF;
    IF work.candidate_id IS NULL THEN
        SELECT GREATEST(work.base_revision,
            COALESCE((SELECT max(candidate_revision)
                FROM __forest_module_schema__.vector_rebuild_attempts
                WHERE bot_id=requested_bot),0),
            COALESCE((SELECT max(binding_revision)
                FROM __forest_module_schema__.vector_collection_bindings
                WHERE bot_id=requested_bot),0))+1 INTO next_revision;
        base_name := 'forest_memory_ref2_'||substr(encode(sha256(
            convert_to(requested_bot,'UTF8')||decode('00','hex')||
            convert_to(work.space_id,'UTF8')),'hex'),1,32);
        next_name := base_name||'_r'||next_revision::text;
        INSERT INTO __forest_module_schema__.vector_rebuild_attempts(
            bot_id,candidate_revision,request_id,attempt,collection_name
        ) VALUES(requested_bot,next_revision,work.request_id,
                 work.attempt_count+1,next_name);
    ELSE
        next_revision := work.candidate_revision;
        next_name := work.candidate_name;
    END IF;
    work_key := 'vector-rebuild-create:'||encode(sha256(convert_to(
        requested_bot||'/'||work.request_id,'UTF8')),'hex');
    next_payload := jsonb_build_object(
        'protocol','forest.owned-work-claim/1',
        'work_id',work.request_id,'claim_token',requested_token,
        'attempt',work.attempt_count+1,
        'operation_id',work_key||':'||(work.attempt_count+1)::text,
        'causal_root_kind','vector.rebuild',
        'causal_root_id',work.request_id,
        'payload',jsonb_build_object(
            'request_id',work.request_id,'claim_token',requested_token,
            'phase',CASE WHEN work.candidate_id IS NULL THEN 'CREATE'
                         ELSE 'CONFIRMED' END,
            'space_id',work.space_id,'dimensions',work.dimensions,
            'candidate_revision',next_revision,
            'collection_name',next_name,
            'collection_id',work.candidate_id));
    UPDATE __forest_module_schema__.vector_rebuild_work
    SET state='CLAIMED',claim_token=requested_token,
        lease_deadline=clock_timestamp()+(requested_lease*interval '1 second'),
        claim_payload=next_payload,attempt_count=attempt_count+1,
        candidate_revision=next_revision,candidate_name=next_name,
        error_code=NULL,updated_at=clock_timestamp()
    WHERE bot_id=requested_bot AND request_id=work.request_id;
    RETURN jsonb_build_object('protocol','forest.domain-result/1',
        'ok',true,'value',next_payload);
END;
$$;

CREATE FUNCTION vector_confirm_rebuild_candidate(context jsonb, input jsonb)
RETURNS jsonb LANGUAGE plpgsql VOLATILE SET search_path=pg_catalog AS $$
DECLARE
    requested_bot text := context->>'bot_id';
    requested_id bigint := (input->>'collection_id')::bigint;
    work __forest_module_schema__.vector_rebuild_work%ROWTYPE;
    attempt __forest_module_schema__.vector_rebuild_attempts%ROWTYPE;
BEGIN
    IF requested_bot IS NULL OR requested_id IS NULL OR requested_id<=0 THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object('code','VECTOR_REBUILD_CONFIRM_INVALID',
                'message','Candidate identity is invalid','status',422));
    END IF;
    PERFORM pg_advisory_xact_lock(hashtextextended('vector-rebuild:'||requested_bot,0));
    SELECT * INTO work FROM __forest_module_schema__.vector_rebuild_work
    WHERE bot_id=requested_bot AND request_id=input->>'request_id' FOR UPDATE;
    IF NOT FOUND OR work.state<>'CLAIMED'
       OR work.claim_token IS DISTINCT FROM input->>'claim_token'
       OR work.lease_deadline<=clock_timestamp()
       OR work.candidate_revision IS DISTINCT FROM
          (input->>'candidate_revision')::bigint
       OR work.candidate_name IS DISTINCT FROM input->>'collection_name'
       OR NOT EXISTS(
            SELECT 1 FROM __forest_module_schema__.vector_collection_health h
            WHERE h.bot_id=requested_bot
              AND h.binding_revision=work.base_revision) THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object('code','VECTOR_REBUILD_CLAIM_STALE',
                'message','Candidate claim is no longer current','status',409));
    END IF;
    SELECT * INTO attempt FROM __forest_module_schema__.vector_rebuild_attempts
    WHERE bot_id=requested_bot AND candidate_revision=work.candidate_revision
    FOR UPDATE;
    IF NOT FOUND OR attempt.request_id IS DISTINCT FROM work.request_id
       OR attempt.collection_name IS DISTINCT FROM work.candidate_name
       OR (attempt.collection_id IS NOT NULL
           AND attempt.collection_id<>requested_id)
       OR (work.candidate_id IS NOT NULL AND work.candidate_id<>requested_id) THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object('code','VECTOR_REBUILD_IDENTITY_CONFLICT',
                'message','Candidate collection identity differs','status',409));
    END IF;
    UPDATE __forest_module_schema__.vector_rebuild_attempts
    SET collection_id=requested_id
    WHERE bot_id=requested_bot AND candidate_revision=work.candidate_revision;
    UPDATE __forest_module_schema__.vector_rebuild_work
    SET candidate_id=requested_id,updated_at=clock_timestamp()
    WHERE bot_id=requested_bot AND request_id=work.request_id;
    RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',true,
        'value',jsonb_build_object('space_id',work.space_id,
            'candidate_revision',work.candidate_revision,
            'collection_name',work.candidate_name,'collection_id',requested_id));
END;
$$;

CREATE FUNCTION vector_settle_rebuild_create(context jsonb, input jsonb)
RETURNS jsonb LANGUAGE plpgsql VOLATILE SET search_path=pg_catalog AS $$
DECLARE
    requested_bot text := context->>'bot_id';
    prior __forest_module_schema__.vector_rebuild_settlements%ROWTYPE;
    work __forest_module_schema__.vector_rebuild_work%ROWTYPE;
    result_payload jsonb;
    receipt jsonb;
    next_state text;
BEGIN
    IF input->>'protocol' IS DISTINCT FROM 'forest.owned-work-settle-request/1'
       OR input#>>'{outcome,status}' IS NULL
       OR input#>>'{outcome,status}' NOT IN ('SUCCEEDED','FAILED') THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object('code','VECTOR_REBUILD_SETTLE_INVALID',
                'message','Rebuild settlement is invalid','status',422));
    END IF;
    PERFORM pg_advisory_xact_lock(hashtextextended('vector-rebuild:'||requested_bot,0));
    SELECT * INTO prior FROM __forest_module_schema__.vector_rebuild_settlements
    WHERE bot_id=requested_bot AND claim_token=input->>'claim_token';
    IF FOUND THEN
        IF prior.input_payload=input THEN RETURN prior.result_payload; END IF;
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object('code','VECTOR_REBUILD_SETTLE_CONFLICT',
                'message','Candidate claim was settled differently','status',409));
    END IF;
    SELECT * INTO work FROM __forest_module_schema__.vector_rebuild_work
    WHERE bot_id=requested_bot AND request_id=input->>'work_id' FOR UPDATE;
    IF NOT FOUND OR work.state<>'CLAIMED'
       OR work.claim_token IS DISTINCT FROM input->>'claim_token'
       OR work.lease_deadline<=clock_timestamp()
       OR work.attempt_count IS DISTINCT FROM (input->>'attempt')::integer THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object('code','VECTOR_REBUILD_CLAIM_STALE',
                'message','Candidate claim is no longer current','status',409));
    END IF;
    receipt := input#>'{outcome,result}';
    IF input#>>'{outcome,status}'='SUCCEEDED' THEN
        IF work.candidate_id IS NULL
           OR receipt->>'space_id' IS DISTINCT FROM work.space_id
           OR receipt->>'collection_name' IS DISTINCT FROM work.candidate_name
           OR (receipt->>'candidate_revision')::bigint
               IS DISTINCT FROM work.candidate_revision
           OR (receipt->>'collection_id')::bigint
               IS DISTINCT FROM work.candidate_id THEN
            RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
                'error',jsonb_build_object('code','VECTOR_REBUILD_SETTLE_INVALID',
                    'message','Candidate result differs from confirmed identity',
                    'status',422));
        END IF;
        next_state := 'CANDIDATE_READY';
    ELSE
        next_state := CASE WHEN work.attempt_count>=5 THEN 'FAILED'
                           ELSE 'QUEUED' END;
    END IF;
    UPDATE __forest_module_schema__.vector_rebuild_work
    SET state=next_state,claim_token=NULL,lease_deadline=NULL,
        claim_payload=NULL,updated_at=clock_timestamp(),
        replay_waterline=CASE WHEN next_state='CANDIDATE_READY' THEN
            COALESCE((SELECT current_seq FROM __forest_module_schema__.vector_ref_clocks
                      WHERE bot_id=requested_bot),0)
            ELSE replay_waterline END,
        error_code=CASE WHEN next_state='CANDIDATE_READY' THEN NULL
            ELSE COALESCE(input#>>'{outcome,error,code}',
                          'VECTOR_REBUILD_CREATE_FAILED') END
    WHERE bot_id=requested_bot AND request_id=work.request_id;
    result_payload := jsonb_build_object('protocol','forest.domain-result/1',
        'ok',true,'value',jsonb_build_object(
            'protocol','forest.owned-work-settled/1','work_id',work.request_id));
    INSERT INTO __forest_module_schema__.vector_rebuild_settlements(
        bot_id,claim_token,input_payload,result_payload
    ) VALUES(requested_bot,input->>'claim_token',input,result_payload);
    RETURN result_payload;
END;
$$;

-- A replay claim covers at most sixteen ordered committed changes. Each claim
-- owns one physical candidate generation. Losing its lease abandons that
-- candidate, so a late Milvus write cannot affect its successor.
CREATE TABLE vector_rebuild_replay_settlements (
    bot_id text NOT NULL REFERENCES forest_kernel.bot_identity(bot_id),
    claim_token text NOT NULL CHECK (octet_length(claim_token) BETWEEN 1 AND 256),
    input_payload jsonb NOT NULL,
    result_payload jsonb NOT NULL,
    settled_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY(bot_id,claim_token)
);
CREATE INDEX vector_rebuild_replay_settlements_gc_idx
    ON vector_rebuild_replay_settlements(bot_id,settled_at,claim_token);
CREATE TRIGGER vector_rebuild_replay_settlement_guard
    BEFORE UPDATE OR DELETE ON vector_rebuild_replay_settlements
    FOR EACH ROW EXECUTE FUNCTION guard_vector_rebuild_settlement();
CREATE FUNCTION vector_claim_rebuild_replay(context jsonb, input jsonb)
RETURNS jsonb LANGUAGE plpgsql VOLATILE SET search_path=pg_catalog AS $$
DECLARE
    requested_bot text := context->>'bot_id';
    requested_token text := input->>'claim_token';
    requested_lease integer := (input->>'lease_seconds')::integer;
    work __forest_module_schema__.vector_rebuild_work%ROWTYPE;
    head_seq bigint;
    page_count integer;
    first_seq bigint;
    last_seq bigint;
    actions jsonb;
    next_payload jsonb;
    work_key text;
BEGIN
    IF input->>'protocol' IS DISTINCT FROM 'forest.owned-work-claim-request/1'
       OR requested_bot IS NULL OR requested_token IS NULL
       OR octet_length(requested_token) NOT BETWEEN 1 AND 256
       OR requested_lease IS NULL OR requested_lease NOT BETWEEN 3 AND 86400 THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object('code','VECTOR_REPLAY_CLAIM_INVALID',
                'message','Replay claim is invalid','status',422));
    END IF;
    PERFORM pg_advisory_xact_lock(hashtextextended('vector-rebuild:'||requested_bot,0));
    DELETE FROM __forest_module_schema__.vector_rebuild_replay_settlements
    WHERE bot_id=requested_bot AND claim_token IN (
        SELECT claim_token FROM __forest_module_schema__.vector_rebuild_replay_settlements
        WHERE bot_id=requested_bot
          AND settled_at<clock_timestamp()-interval '48 hours'
        ORDER BY settled_at,claim_token LIMIT 16
    );
    SELECT * INTO work FROM __forest_module_schema__.vector_rebuild_work
    WHERE bot_id=requested_bot AND state='REPLAY_CLAIMED'
    ORDER BY updated_at LIMIT 1 FOR UPDATE;
    IF FOUND AND work.claim_token=requested_token
       AND work.lease_deadline>clock_timestamp() THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1',
            'ok',true,'value',work.claim_payload);
    END IF;
    IF FOUND AND work.lease_deadline<=clock_timestamp() THEN
        UPDATE __forest_module_schema__.vector_rebuild_work
        SET state=CASE WHEN attempt_count>=5 THEN 'FAILED' ELSE 'QUEUED' END,
            candidate_revision=NULL,candidate_name=NULL,candidate_id=NULL,
            replay_waterline=NULL,replay_cursor=0,claim_token=NULL,
            lease_deadline=NULL,claim_payload=NULL,
            error_code='VECTOR_REPLAY_LEASE_EXPIRED',
            updated_at=clock_timestamp()
        WHERE bot_id=requested_bot AND request_id=work.request_id;
    END IF;
    SELECT * INTO work FROM __forest_module_schema__.vector_rebuild_work
    WHERE bot_id=requested_bot AND state IN ('CANDIDATE_READY','AUDITED')
    ORDER BY created_at LIMIT 1 FOR UPDATE;
    IF NOT FOUND THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1',
            'ok',true,'value',NULL);
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM __forest_module_schema__.vector_collection_health h
        WHERE h.bot_id=requested_bot AND h.binding_revision=work.base_revision
    ) THEN
        UPDATE __forest_module_schema__.vector_rebuild_work
        SET state='FAILED',error_code='VECTOR_REBUILD_BASE_CHANGED',
            updated_at=clock_timestamp()
        WHERE bot_id=requested_bot AND request_id=work.request_id;
        RETURN jsonb_build_object('protocol','forest.domain-result/1',
            'ok',true,'value',NULL);
    END IF;
    SELECT COALESCE(current_seq,0) INTO head_seq
    FROM __forest_module_schema__.vector_ref_clocks
    WHERE bot_id=requested_bot;
    head_seq := COALESCE(head_seq,0);
    IF work.replay_cursor>=head_seq THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1',
            'ok',true,'value',NULL);
    END IF;
    IF work.state='AUDITED' THEN
        UPDATE __forest_module_schema__.vector_rebuild_work
        SET state='CANDIDATE_READY',audit_seq=NULL,audit_after_ref=NULL,
            audited_at=NULL,audit_failure_count=0,updated_at=clock_timestamp()
        WHERE bot_id=requested_bot AND request_id=work.request_id;
    END IF;
    SELECT count(*),min(mutation_seq),max(mutation_seq),
           COALESCE(jsonb_agg(jsonb_build_object(
                'mutation_seq',mutation_seq,'ref_id',ref_id,
                'space_id',space_id,'state',effective_state,
                'purpose',CASE WHEN effective_state='ACTIVE' THEN purpose END,
                'input_digest',CASE WHEN effective_state='ACTIVE' THEN input_digest END,
                'replay_locator',CASE WHEN effective_state='ACTIVE' THEN replay_locator END,
                'vector_digest',CASE WHEN effective_state='ACTIVE' THEN vector_digest END,
                'physical_digest',CASE WHEN effective_state='ACTIVE' THEN physical_digest END
           ) ORDER BY mutation_seq),'[]'::jsonb)
    INTO page_count,first_seq,last_seq,actions
    FROM (
        SELECT c.mutation_seq,c.ref_id,c.space_id,c.purpose,c.input_digest,
               c.replay_locator,c.vector_digest,c.physical_digest,
               CASE WHEN c.state='ACTIVE' AND r.state='ACTIVE'
                    THEN 'ACTIVE' ELSE 'TOMBSTONED' END AS effective_state
        FROM __forest_module_schema__.vector_ref_changes c
        JOIN __forest_module_schema__.vector_index_refs r
          ON r.bot_id=c.bot_id AND r.ref_id=c.ref_id
        WHERE c.bot_id=requested_bot AND c.mutation_seq>work.replay_cursor
          AND c.mutation_seq<=head_seq
        ORDER BY c.mutation_seq LIMIT 16
    ) page;
    IF page_count=0 OR first_seq<>work.replay_cursor+1
       OR last_seq<>work.replay_cursor+page_count THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object('code','VECTOR_REF_CHANGE_GAP',
                'message','Ref change ledger has a gap','status',503));
    END IF;
    work_key := 'vector-rebuild-replay:'||encode(sha256(convert_to(
        requested_bot||'/'||work.request_id,'UTF8')),'hex');
    next_payload := jsonb_build_object(
        'protocol','forest.owned-work-claim/1','work_id',work.request_id,
        'claim_token',requested_token,'attempt',work.replay_attempt_count+1,
        'operation_id',work_key||':'||work.candidate_revision::text||':'||last_seq::text,
        'causal_root_kind','vector.rebuild','causal_root_id',work.request_id,
        'payload',jsonb_build_object(
            'request_id',work.request_id,'space_id',work.space_id,
            'candidate_revision',work.candidate_revision,
            'collection_id',work.candidate_id,
            'collection_name',work.candidate_name,
            'after_seq',work.replay_cursor,'next_seq',last_seq,
            'changes',actions));
    UPDATE __forest_module_schema__.vector_rebuild_work
    SET state='REPLAY_CLAIMED',claim_token=requested_token,
        lease_deadline=clock_timestamp()+(requested_lease*interval '1 second'),
        claim_payload=next_payload,replay_attempt_count=replay_attempt_count+1,
        updated_at=clock_timestamp()
    WHERE bot_id=requested_bot AND request_id=work.request_id;
    RETURN jsonb_build_object('protocol','forest.domain-result/1',
        'ok',true,'value',next_payload);
END;
$$;

CREATE FUNCTION vector_settle_rebuild_replay(context jsonb, input jsonb)
RETURNS jsonb LANGUAGE plpgsql VOLATILE SET search_path=pg_catalog AS $$
DECLARE
    requested_bot text := context->>'bot_id';
    prior __forest_module_schema__.vector_rebuild_replay_settlements%ROWTYPE;
    work __forest_module_schema__.vector_rebuild_work%ROWTYPE;
    receipt jsonb;
    response jsonb;
    succeeded boolean;
BEGIN
    IF input->>'protocol' IS DISTINCT FROM 'forest.owned-work-settle-request/1'
       OR input#>>'{outcome,status}' IS NULL
       OR input#>>'{outcome,status}' NOT IN ('SUCCEEDED','FAILED') THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object('code','VECTOR_REPLAY_SETTLE_INVALID',
                'message','Replay settlement is invalid','status',422));
    END IF;
    PERFORM pg_advisory_xact_lock(hashtextextended('vector-rebuild:'||requested_bot,0));
    SELECT * INTO prior FROM __forest_module_schema__.vector_rebuild_replay_settlements
    WHERE bot_id=requested_bot AND claim_token=input->>'claim_token';
    IF FOUND THEN
        IF prior.input_payload=input THEN RETURN prior.result_payload; END IF;
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object('code','VECTOR_REPLAY_SETTLE_CONFLICT',
                'message','Replay claim was settled differently','status',409));
    END IF;
    SELECT * INTO work FROM __forest_module_schema__.vector_rebuild_work
    WHERE bot_id=requested_bot AND request_id=input->>'work_id' FOR UPDATE;
    IF NOT FOUND OR work.state<>'REPLAY_CLAIMED'
       OR work.claim_token IS DISTINCT FROM input->>'claim_token'
       OR work.lease_deadline<=clock_timestamp()
       OR work.replay_attempt_count IS DISTINCT FROM (input->>'attempt')::bigint THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object('code','VECTOR_REPLAY_CLAIM_STALE',
                'message','Replay claim is no longer current','status',409));
    END IF;
    succeeded := input#>>'{outcome,status}'='SUCCEEDED';
    receipt := input#>'{outcome,result}';
    IF succeeded AND (
        receipt->>'request_id' IS DISTINCT FROM work.request_id
        OR (receipt->>'candidate_revision')::bigint IS DISTINCT FROM work.candidate_revision
        OR (receipt->>'collection_id')::bigint IS DISTINCT FROM work.candidate_id
        OR (receipt->>'next_seq')::bigint IS DISTINCT FROM
            (work.claim_payload#>>'{payload,next_seq}')::bigint
    ) THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object('code','VECTOR_REPLAY_SETTLE_INVALID',
                'message','Replay receipt differs from claimed page','status',422));
    END IF;
    UPDATE __forest_module_schema__.vector_rebuild_work
    SET state=CASE WHEN succeeded THEN 'CANDIDATE_READY'
                   WHEN attempt_count>=5 THEN 'FAILED' ELSE 'QUEUED' END,
        replay_cursor=CASE WHEN succeeded THEN (receipt->>'next_seq')::bigint
                           ELSE 0 END,
        candidate_revision=CASE WHEN succeeded THEN candidate_revision END,
        candidate_name=CASE WHEN succeeded THEN candidate_name END,
        candidate_id=CASE WHEN succeeded THEN candidate_id END,
        replay_waterline=CASE WHEN succeeded THEN replay_waterline END,
        audit_seq=NULL,audit_after_ref=NULL,audited_at=NULL,
        audit_failure_count=0,claim_token=NULL,lease_deadline=NULL,claim_payload=NULL,
        error_code=CASE WHEN succeeded THEN NULL ELSE
            COALESCE(input#>>'{outcome,error,code}','VECTOR_REPLAY_FAILED') END,
        updated_at=clock_timestamp()
    WHERE bot_id=requested_bot AND request_id=work.request_id;
    response := jsonb_build_object('protocol','forest.domain-result/1','ok',true,
        'value',jsonb_build_object('protocol','forest.owned-work-settled/1',
            'work_id',work.request_id));
    INSERT INTO __forest_module_schema__.vector_rebuild_replay_settlements(
        bot_id,claim_token,input_payload,result_payload
    ) VALUES(requested_bot,input->>'claim_token',input,response);
    RETURN response;
END;
$$;

-- Candidate audit holds replay paused while checking a single immutable ref
-- waterline. A newer committed ref transition invalidates AUDITED before
-- another replay page is claimed; final publication is a separate fence.
CREATE INDEX vector_ref_changes_tombstone_ref_idx
    ON vector_ref_changes(bot_id,ref_id,mutation_seq)
    WHERE state='TOMBSTONED';
CREATE TABLE vector_rebuild_audit_settlements (
    bot_id text NOT NULL REFERENCES forest_kernel.bot_identity(bot_id),
    claim_token text NOT NULL CHECK (octet_length(claim_token) BETWEEN 1 AND 256),
    input_payload jsonb NOT NULL,
    result_payload jsonb NOT NULL,
    settled_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY(bot_id,claim_token)
);
CREATE INDEX vector_rebuild_audit_settlements_gc_idx
    ON vector_rebuild_audit_settlements(bot_id,settled_at,claim_token);
CREATE TRIGGER vector_rebuild_audit_settlement_guard
    BEFORE UPDATE OR DELETE ON vector_rebuild_audit_settlements
    FOR EACH ROW EXECUTE FUNCTION guard_vector_rebuild_settlement();

CREATE FUNCTION vector_claim_rebuild_audit(context jsonb, input jsonb)
RETURNS jsonb LANGUAGE plpgsql VOLATILE SET search_path=pg_catalog AS $$
DECLARE
    requested_bot text := context->>'bot_id';
    requested_token text := input->>'claim_token';
    requested_lease integer := (input->>'lease_seconds')::integer;
    work __forest_module_schema__.vector_rebuild_work%ROWTYPE;
    head_seq bigint;
    expected_rows jsonb;
    next_payload jsonb;
    work_key text;
BEGIN
    IF input->>'protocol' IS DISTINCT FROM 'forest.owned-work-claim-request/1'
       OR requested_bot IS NULL OR requested_token IS NULL
       OR octet_length(requested_token) NOT BETWEEN 1 AND 256
       OR requested_lease IS NULL OR requested_lease NOT BETWEEN 3 AND 86400 THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object('code','VECTOR_REBUILD_AUDIT_CLAIM_INVALID',
                'message','Candidate audit claim is invalid','status',422));
    END IF;
    PERFORM pg_advisory_xact_lock(hashtextextended('vector-rebuild:'||requested_bot,0));
    DELETE FROM __forest_module_schema__.vector_rebuild_audit_settlements
    WHERE bot_id=requested_bot AND claim_token IN (
        SELECT claim_token FROM __forest_module_schema__.vector_rebuild_audit_settlements
        WHERE bot_id=requested_bot
          AND settled_at<clock_timestamp()-interval '48 hours'
        ORDER BY settled_at,claim_token LIMIT 16
    );
    SELECT * INTO work FROM __forest_module_schema__.vector_rebuild_work
    WHERE bot_id=requested_bot AND state='AUDIT_CLAIMED'
    ORDER BY updated_at LIMIT 1 FOR UPDATE;
    IF FOUND AND work.claim_token=requested_token
       AND work.lease_deadline>clock_timestamp() THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1',
            'ok',true,'value',work.claim_payload);
    END IF;
    IF FOUND AND work.lease_deadline<=clock_timestamp() THEN
        UPDATE __forest_module_schema__.vector_rebuild_work
        SET state='AUDIT_READY',claim_token=NULL,lease_deadline=NULL,
            claim_payload=NULL,error_code='VECTOR_REBUILD_AUDIT_LEASE_EXPIRED',
            updated_at=clock_timestamp()
        WHERE bot_id=requested_bot AND request_id=work.request_id;
    END IF;
    SELECT * INTO work FROM __forest_module_schema__.vector_rebuild_work
    WHERE bot_id=requested_bot AND state IN ('CANDIDATE_READY','AUDIT_READY')
    ORDER BY created_at LIMIT 1 FOR UPDATE;
    IF NOT FOUND THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1',
            'ok',true,'value',NULL);
    END IF;
    IF work.state='CANDIDATE_READY' THEN
        SELECT COALESCE(current_seq,0) INTO head_seq
        FROM __forest_module_schema__.vector_ref_clocks
        WHERE bot_id=requested_bot;
        head_seq := COALESCE(head_seq,0);
        IF head_seq<>work.replay_cursor THEN
            RETURN jsonb_build_object('protocol','forest.domain-result/1',
                'ok',true,'value',NULL);
        END IF;
        UPDATE __forest_module_schema__.vector_rebuild_work
        SET state='AUDIT_READY',audit_seq=head_seq,audit_after_ref=NULL,
            audit_failure_count=0,updated_at=clock_timestamp()
        WHERE bot_id=requested_bot AND request_id=work.request_id;
        work.audit_seq := head_seq;
        work.audit_after_ref := NULL;
    END IF;
    SELECT COALESCE(jsonb_agg(to_jsonb(page) ORDER BY ref_id),'[]'::jsonb)
    INTO expected_rows
    FROM (
        SELECT c.ref_id,c.bot_id,c.space_id,c.purpose,
               c.input_digest,c.physical_digest
        FROM __forest_module_schema__.vector_ref_changes c
        WHERE c.bot_id=requested_bot AND c.state='ACTIVE'
          AND c.mutation_seq<=work.audit_seq
          AND (work.audit_after_ref IS NULL OR c.ref_id>work.audit_after_ref)
          AND NOT EXISTS (
              SELECT 1 FROM __forest_module_schema__.vector_ref_changes d
              WHERE d.bot_id=c.bot_id AND d.ref_id=c.ref_id
                AND d.state='TOMBSTONED' AND d.mutation_seq<=work.audit_seq
          )
        ORDER BY c.ref_id LIMIT 64
    ) page;
    work_key := 'vector-rebuild-audit:'||encode(sha256(convert_to(
        requested_bot||'/'||work.request_id,'UTF8')),'hex');
    next_payload := jsonb_build_object(
        'protocol','forest.owned-work-claim/1','work_id',work.request_id,
        'claim_token',requested_token,'attempt',work.audit_claim_count+1,
        'operation_id',work_key||':'||work.candidate_revision::text||':'||
            (work.audit_claim_count+1)::text,
        'causal_root_kind','vector.rebuild','causal_root_id',work.request_id,
        'payload',jsonb_build_object(
            'request_id',work.request_id,'space_id',work.space_id,
            'candidate_revision',work.candidate_revision,
            'collection_id',work.candidate_id,
            'collection_name',work.candidate_name,
            'audit_seq',work.audit_seq,'after_ref',work.audit_after_ref,
            'limit',64,'expected',expected_rows));
    UPDATE __forest_module_schema__.vector_rebuild_work
    SET state='AUDIT_CLAIMED',claim_token=requested_token,
        lease_deadline=clock_timestamp()+(requested_lease*interval '1 second'),
        claim_payload=next_payload,audit_claim_count=audit_claim_count+1,
        updated_at=clock_timestamp()
    WHERE bot_id=requested_bot AND request_id=work.request_id;
    RETURN jsonb_build_object('protocol','forest.domain-result/1',
        'ok',true,'value',next_payload);
END;
$$;

CREATE FUNCTION vector_settle_rebuild_audit(context jsonb, input jsonb)
RETURNS jsonb LANGUAGE plpgsql VOLATILE SET search_path=pg_catalog AS $$
DECLARE
    requested_bot text := context->>'bot_id';
    prior __forest_module_schema__.vector_rebuild_audit_settlements%ROWTYPE;
    work __forest_module_schema__.vector_rebuild_work%ROWTYPE;
    receipt jsonb;
    response jsonb;
    succeeded boolean;
    page jsonb;
    expected jsonb;
    expected_last text;
BEGIN
    IF input->>'protocol' IS DISTINCT FROM 'forest.owned-work-settle-request/1'
       OR input#>>'{outcome,status}' IS NULL
       OR input#>>'{outcome,status}' NOT IN ('SUCCEEDED','FAILED') THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object('code','VECTOR_REBUILD_AUDIT_SETTLE_INVALID',
                'message','Candidate audit settlement is invalid','status',422));
    END IF;
    PERFORM pg_advisory_xact_lock(hashtextextended('vector-rebuild:'||requested_bot,0));
    SELECT * INTO prior FROM __forest_module_schema__.vector_rebuild_audit_settlements
    WHERE bot_id=requested_bot AND claim_token=input->>'claim_token';
    IF FOUND THEN
        IF prior.input_payload=input THEN RETURN prior.result_payload; END IF;
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object('code','VECTOR_REBUILD_AUDIT_SETTLE_CONFLICT',
                'message','Audit claim was settled differently','status',409));
    END IF;
    SELECT * INTO work FROM __forest_module_schema__.vector_rebuild_work
    WHERE bot_id=requested_bot AND request_id=input->>'work_id' FOR UPDATE;
    IF NOT FOUND OR work.state<>'AUDIT_CLAIMED'
       OR work.claim_token IS DISTINCT FROM input->>'claim_token'
       OR work.lease_deadline<=clock_timestamp()
       OR work.audit_claim_count IS DISTINCT FROM (input->>'attempt')::bigint THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object('code','VECTOR_REBUILD_AUDIT_CLAIM_STALE',
                'message','Candidate audit claim is no longer current','status',409));
    END IF;
    succeeded := input#>>'{outcome,status}'='SUCCEEDED';
    receipt := input#>'{outcome,result}';
    page := work.claim_payload->'payload';
    expected := page->'expected';
    IF succeeded THEN
        expected_last := CASE WHEN jsonb_array_length(expected)>0 THEN
            expected->(jsonb_array_length(expected)-1)->>'ref_id'
            ELSE page->>'after_ref' END;
        IF receipt->>'request_id' IS DISTINCT FROM work.request_id
           OR (receipt->>'candidate_revision')::bigint IS DISTINCT FROM work.candidate_revision
           OR (receipt->>'collection_id')::bigint IS DISTINCT FROM work.candidate_id
           OR (receipt->>'audit_seq')::bigint IS DISTINCT FROM work.audit_seq
           OR receipt->>'next_ref' IS DISTINCT FROM expected_last
           OR (receipt->>'done')::boolean IS DISTINCT FROM
              (jsonb_array_length(expected)<64) THEN
            RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
                'error',jsonb_build_object('code','VECTOR_REBUILD_AUDIT_SETTLE_INVALID',
                    'message','Audit result differs from claimed page','status',422));
        END IF;
    END IF;
    UPDATE __forest_module_schema__.vector_rebuild_work
    SET state=CASE WHEN succeeded AND (receipt->>'done')::boolean THEN 'AUDITED'
                   WHEN succeeded THEN 'AUDIT_READY'
                   WHEN input#>>'{outcome,error,code}'='VECTOR_REBUILD_AUDIT_MISMATCH'
                        OR audit_failure_count>=2 THEN 'FAILED'
                   ELSE 'AUDIT_READY' END,
        audit_after_ref=CASE WHEN succeeded THEN receipt->>'next_ref'
                             ELSE audit_after_ref END,
        audit_failure_count=CASE WHEN succeeded THEN 0
                                 ELSE audit_failure_count+1 END,
        audited_at=CASE WHEN succeeded AND (receipt->>'done')::boolean
                        THEN clock_timestamp() ELSE NULL END,
        claim_token=NULL,lease_deadline=NULL,claim_payload=NULL,
        error_code=CASE WHEN succeeded THEN NULL ELSE
            COALESCE(input#>>'{outcome,error,code}',
                     'VECTOR_REBUILD_AUDIT_FAILED') END,
        updated_at=clock_timestamp()
    WHERE bot_id=requested_bot AND request_id=work.request_id;
    response := jsonb_build_object('protocol','forest.domain-result/1','ok',true,
        'value',jsonb_build_object('protocol','forest.owned-work-settled/1',
            'work_id',work.request_id));
    INSERT INTO __forest_module_schema__.vector_rebuild_audit_settlements(
        bot_id,claim_token,input_payload,result_payload
    ) VALUES(requested_bot,input->>'claim_token',input,response);
    RETURN response;
END;
$$;

-- A short SQL admission records physical calls against their pinned active
-- binding. Leases bound crashed callers; late effects still target only the
-- old physical collection and their SQL publication check rejects it.
CREATE TABLE vector_io_tickets (
    bot_id text NOT NULL REFERENCES forest_kernel.bot_identity(bot_id),
    ticket text NOT NULL CHECK (octet_length(ticket) BETWEEN 1 AND 256),
    binding_revision bigint NOT NULL,
    collection_id bigint NOT NULL CHECK (collection_id>0),
    purpose text NOT NULL CHECK (purpose IN ('UPSERT','SWEEP')),
    expires_at timestamptz NOT NULL,
    released_at timestamptz,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY(bot_id,ticket),
    FOREIGN KEY(bot_id,binding_revision)
        REFERENCES vector_collection_bindings(bot_id,binding_revision)
);
CREATE INDEX vector_io_tickets_active_idx ON vector_io_tickets(
    bot_id,expires_at,ticket
) WHERE released_at IS NULL;
CREATE INDEX vector_io_tickets_gc_idx ON vector_io_tickets(
    bot_id,created_at,ticket
);

CREATE FUNCTION vector_io_enter(context jsonb, input jsonb)
RETURNS jsonb LANGUAGE plpgsql VOLATILE SET search_path=pg_catalog AS $$
DECLARE
    requested_bot text := context->>'bot_id';
    requested_ticket text := context->>'operation_id';
    requested_space text := input->>'space_id';
    requested_purpose text := input->>'purpose';
    requested_binding jsonb := input->'binding';
    existing __forest_module_schema__.vector_io_tickets%ROWTYPE;
    binding __forest_module_schema__.vector_collection_bindings%ROWTYPE;
BEGIN
    IF requested_bot IS NULL OR requested_ticket IS NULL
       OR octet_length(requested_ticket) NOT BETWEEN 1 AND 256
       OR requested_space IS NULL OR octet_length(requested_space) NOT BETWEEN 1 AND 256
       OR requested_purpose IS NULL
       OR requested_purpose NOT IN ('UPSERT','SWEEP')
       OR jsonb_typeof(requested_binding) IS DISTINCT FROM 'object' THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object('code','VECTOR_IO_INPUT_INVALID',
                'message','Physical admission is invalid','status',422));
    END IF;
    PERFORM pg_advisory_xact_lock_shared(hashtextextended(
        'vector-collection:'||requested_bot,0));
    DELETE FROM __forest_module_schema__.vector_io_tickets
    WHERE bot_id=requested_bot AND ticket IN (
        SELECT ticket FROM __forest_module_schema__.vector_io_tickets
        WHERE bot_id=requested_bot AND created_at<clock_timestamp()-interval '48 hours'
          AND (released_at IS NOT NULL OR expires_at<clock_timestamp())
        ORDER BY created_at,ticket LIMIT 32
    );
    SELECT * INTO existing FROM __forest_module_schema__.vector_io_tickets
    WHERE bot_id=requested_bot AND ticket=requested_ticket;
    IF FOUND THEN
        IF existing.released_at IS NULL AND existing.expires_at>clock_timestamp()
           AND existing.purpose=requested_purpose
           AND existing.binding_revision=(requested_binding->>'binding_revision')::bigint
           AND existing.collection_id=(requested_binding->>'collection_id')::bigint THEN
            RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',true,
                'value',jsonb_build_object('ticket',requested_ticket,
                    'binding',requested_binding));
        END IF;
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object('code','VECTOR_IO_TICKET_CONFLICT',
                'message','Physical admission token was reused','status',409));
    END IF;
    IF EXISTS (
        SELECT 1 FROM __forest_module_schema__.vector_rebuild_work
        WHERE bot_id=requested_bot AND state='PUBLISHING'
    ) THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object('code','VECTOR_INDEX_SWITCHING',
                'message','Vector binding is publishing','status',503));
    END IF;
    SELECT b.* INTO binding
    FROM __forest_module_schema__.vector_collection_health h
    JOIN __forest_module_schema__.vector_collection_bindings b
      ON b.bot_id=h.bot_id AND b.binding_revision=h.binding_revision
    WHERE h.bot_id=requested_bot AND b.space_id=requested_space;
    IF NOT FOUND OR binding.binding_revision IS DISTINCT FROM
            (requested_binding->>'binding_revision')::bigint
       OR binding.collection_id IS DISTINCT FROM
            (requested_binding->>'collection_id')::bigint
       OR binding.collection_name IS DISTINCT FROM
            requested_binding->>'collection_name' THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object('code','VECTOR_INDEX_BINDING_STALE',
                'message','Physical admission targets an inactive binding','status',503));
    END IF;
    INSERT INTO __forest_module_schema__.vector_io_tickets(
        bot_id,ticket,binding_revision,collection_id,purpose,expires_at
    ) VALUES(requested_bot,requested_ticket,binding.binding_revision,
             binding.collection_id,requested_purpose,
             clock_timestamp()+interval '90 seconds');
    RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',true,
        'value',jsonb_build_object('ticket',requested_ticket,
            'binding',requested_binding));
END;
$$;

CREATE FUNCTION vector_io_exit(context jsonb, input jsonb)
RETURNS jsonb LANGUAGE plpgsql VOLATILE SET search_path=pg_catalog AS $$
DECLARE
    requested_bot text := context->>'bot_id';
    requested_ticket text := input->>'ticket';
    prior __forest_module_schema__.vector_io_tickets%ROWTYPE;
BEGIN
    SELECT * INTO prior FROM __forest_module_schema__.vector_io_tickets
    WHERE bot_id=requested_bot AND ticket=requested_ticket FOR UPDATE;
    IF NOT FOUND THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object('code','VECTOR_IO_TICKET_MISSING',
                'message','Physical admission ticket is missing','status',409));
    END IF;
    UPDATE __forest_module_schema__.vector_io_tickets
    SET released_at=COALESCE(released_at,clock_timestamp())
    WHERE bot_id=requested_bot AND ticket=requested_ticket;
    RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',true,
        'value',jsonb_build_object('ticket',requested_ticket));
END;
$$;

CREATE TABLE vector_rebuild_publish_settlements (
    bot_id text NOT NULL REFERENCES forest_kernel.bot_identity(bot_id),
    claim_token text NOT NULL CHECK (octet_length(claim_token) BETWEEN 1 AND 256),
    input_payload jsonb NOT NULL,
    result_payload jsonb NOT NULL,
    settled_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY(bot_id,claim_token)
);
CREATE INDEX vector_rebuild_publish_settlements_gc_idx
    ON vector_rebuild_publish_settlements(bot_id,settled_at,claim_token);
CREATE TRIGGER vector_rebuild_publish_settlement_guard
    BEFORE UPDATE OR DELETE ON vector_rebuild_publish_settlements
    FOR EACH ROW EXECUTE FUNCTION guard_vector_rebuild_settlement();

CREATE FUNCTION vector_claim_rebuild_publish(context jsonb, input jsonb)
RETURNS jsonb LANGUAGE plpgsql VOLATILE SET search_path=pg_catalog AS $$
DECLARE
    requested_bot text := context->>'bot_id';
    requested_token text := input->>'claim_token';
    requested_lease integer := (input->>'lease_seconds')::integer;
    work __forest_module_schema__.vector_rebuild_work%ROWTYPE;
    head_seq bigint;
    next_payload jsonb;
    work_key text;
    had_publishing boolean;
BEGIN
    IF input->>'protocol' IS DISTINCT FROM 'forest.owned-work-claim-request/1'
       OR requested_bot IS NULL OR requested_token IS NULL
       OR octet_length(requested_token) NOT BETWEEN 1 AND 256
       OR requested_lease IS NULL OR requested_lease NOT BETWEEN 3 AND 86400 THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object('code','VECTOR_REBUILD_PUBLISH_CLAIM_INVALID',
                'message','Publish claim is invalid','status',422));
    END IF;
    PERFORM pg_advisory_xact_lock(hashtextextended('vector-rebuild:'||requested_bot,0));
    -- The same collection lock is shared by ref mutations and I/O admission.
    -- Closing the gate here cannot race a newly admitted physical call.
    PERFORM pg_advisory_xact_lock(hashtextextended('vector-collection:'||requested_bot,0));
    DELETE FROM __forest_module_schema__.vector_rebuild_publish_settlements
    WHERE bot_id=requested_bot AND claim_token IN (
        SELECT claim_token FROM __forest_module_schema__.vector_rebuild_publish_settlements
        WHERE bot_id=requested_bot
          AND settled_at<clock_timestamp()-interval '48 hours'
        ORDER BY settled_at,claim_token LIMIT 16
    );
    SELECT * INTO work FROM __forest_module_schema__.vector_rebuild_work
    WHERE bot_id=requested_bot AND state='PUBLISHING'
    ORDER BY updated_at LIMIT 1 FOR UPDATE;
    had_publishing := FOUND;
    IF had_publishing AND work.claim_token=requested_token
       AND work.lease_deadline>clock_timestamp() THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1',
            'ok',true,'value',work.claim_payload);
    END IF;
    SELECT COALESCE(current_seq,0) INTO head_seq
    FROM __forest_module_schema__.vector_ref_clocks
    WHERE bot_id=requested_bot;
    head_seq := COALESCE(head_seq,0);
    IF had_publishing AND work.lease_deadline<=clock_timestamp() THEN
        UPDATE __forest_module_schema__.vector_rebuild_work
        SET state=CASE WHEN head_seq=work.audit_seq THEN 'AUDITED'
                       ELSE 'CANDIDATE_READY' END,
            audit_seq=CASE WHEN head_seq=work.audit_seq THEN audit_seq END,
            audit_after_ref=CASE WHEN head_seq=work.audit_seq
                                 THEN audit_after_ref END,
            audited_at=CASE WHEN head_seq=work.audit_seq THEN audited_at END,
            claim_token=NULL,lease_deadline=NULL,claim_payload=NULL,
            error_code='VECTOR_REBUILD_PUBLISH_LEASE_EXPIRED',
            updated_at=clock_timestamp()
        WHERE bot_id=requested_bot AND request_id=work.request_id;
    ELSIF had_publishing THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1',
            'ok',true,'value',NULL);
    END IF;
    SELECT * INTO work FROM __forest_module_schema__.vector_rebuild_work
    WHERE bot_id=requested_bot AND state='AUDITED'
    ORDER BY created_at LIMIT 1 FOR UPDATE;
    IF NOT FOUND THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1',
            'ok',true,'value',NULL);
    END IF;
    IF head_seq<>work.audit_seq THEN
        UPDATE __forest_module_schema__.vector_rebuild_work
        SET state='CANDIDATE_READY',audit_seq=NULL,audit_after_ref=NULL,
            audited_at=NULL,updated_at=clock_timestamp()
        WHERE bot_id=requested_bot AND request_id=work.request_id;
        RETURN jsonb_build_object('protocol','forest.domain-result/1',
            'ok',true,'value',NULL);
    END IF;
    work_key := 'vector-rebuild-publish:'||encode(sha256(convert_to(
        requested_bot||'/'||work.request_id,'UTF8')),'hex');
    next_payload := jsonb_build_object(
        'protocol','forest.owned-work-claim/1','work_id',work.request_id,
        'claim_token',requested_token,'attempt',work.publish_attempt_count+1,
        'operation_id',work_key||':'||(work.publish_attempt_count+1)::text,
        'causal_root_kind','vector.rebuild','causal_root_id',work.request_id,
        'payload',jsonb_build_object(
            'request_id',work.request_id,'claim_token',requested_token,
            'space_id',work.space_id,'base_revision',work.base_revision,
            'candidate_revision',work.candidate_revision,
            'collection_id',work.candidate_id,
            'collection_name',work.candidate_name,
            'audit_seq',work.audit_seq));
    UPDATE __forest_module_schema__.vector_rebuild_work
    SET state='PUBLISHING',claim_token=requested_token,
        lease_deadline=clock_timestamp()+(requested_lease*interval '1 second'),
        claim_payload=next_payload,
        publish_attempt_count=publish_attempt_count+1,
        updated_at=clock_timestamp()
    WHERE bot_id=requested_bot AND request_id=work.request_id;
    RETURN jsonb_build_object('protocol','forest.domain-result/1',
        'ok',true,'value',next_payload);
END;
$$;

CREATE FUNCTION vector_query_publish_drain(context jsonb, input jsonb)
RETURNS jsonb LANGUAGE plpgsql STABLE SET search_path=pg_catalog AS $$
DECLARE
    work __forest_module_schema__.vector_rebuild_work%ROWTYPE;
    active_count bigint;
BEGIN
    SELECT * INTO work FROM __forest_module_schema__.vector_rebuild_work
    WHERE bot_id=context->>'bot_id' AND request_id=input->>'request_id'
      AND state='PUBLISHING' AND claim_token=input->>'claim_token'
      AND lease_deadline>now();
    IF NOT FOUND THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object('code','VECTOR_REBUILD_PUBLISH_CLAIM_STALE',
                'message','Publish claim is no longer current','status',409));
    END IF;
    SELECT count(*) INTO active_count
    FROM __forest_module_schema__.vector_io_tickets
    WHERE bot_id=work.bot_id AND released_at IS NULL AND expires_at>now();
    RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',true,
        'value',jsonb_build_object('active_count',active_count));
END;
$$;

CREATE FUNCTION vector_publish_rebuild(context jsonb, input jsonb)
RETURNS jsonb LANGUAGE plpgsql VOLATILE SET search_path=pg_catalog AS $$
DECLARE
    requested_bot text := context->>'bot_id';
    work __forest_module_schema__.vector_rebuild_work%ROWTYPE;
    health __forest_module_schema__.vector_collection_health%ROWTYPE;
    base __forest_module_schema__.vector_collection_bindings%ROWTYPE;
    head_seq bigint;
    receipt jsonb;
BEGIN
    IF requested_bot IS NULL OR input->>'request_id' IS NULL
       OR input->>'claim_token' IS NULL THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object('code','VECTOR_REBUILD_PUBLISH_INVALID',
                'message','Publish identity is invalid','status',422));
    END IF;
    PERFORM pg_advisory_xact_lock(hashtextextended('vector-rebuild:'||requested_bot,0));
    PERFORM pg_advisory_xact_lock(hashtextextended('vector-collection:'||requested_bot,0));
    SELECT * INTO work FROM __forest_module_schema__.vector_rebuild_work
    WHERE bot_id=requested_bot AND request_id=input->>'request_id' FOR UPDATE;
    IF NOT FOUND OR work.claim_token IS DISTINCT FROM input->>'claim_token'
       OR work.state NOT IN ('PUBLISHING','PUBLISHED')
       OR work.candidate_revision IS DISTINCT FROM
            (input->>'candidate_revision')::bigint
       OR work.candidate_id IS DISTINCT FROM (input->>'collection_id')::bigint
       OR work.candidate_name IS DISTINCT FROM input->>'collection_name'
       OR work.space_id IS DISTINCT FROM input->>'space_id'
       OR work.base_revision IS DISTINCT FROM (input->>'base_revision')::bigint
       OR work.audit_seq IS DISTINCT FROM (input->>'audit_seq')::bigint THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object('code','VECTOR_REBUILD_PUBLISH_CLAIM_STALE',
                'message','Publish claim is no longer current','status',409));
    END IF;
    receipt := jsonb_build_object(
        'request_id',work.request_id,'space_id',work.space_id,
        'binding_revision',work.candidate_revision,
        'collection_id',work.candidate_id,
        'collection_name',work.candidate_name,
        'published_seq',work.audit_seq);
    IF work.state='PUBLISHED' THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1',
            'ok',true,'value',receipt);
    END IF;
    IF work.lease_deadline<=clock_timestamp() THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object('code','VECTOR_REBUILD_PUBLISH_CLAIM_STALE',
                'message','Publish lease expired','status',409));
    END IF;
    SELECT * INTO health FROM __forest_module_schema__.vector_collection_health
    WHERE bot_id=requested_bot FOR UPDATE;
    SELECT * INTO base FROM __forest_module_schema__.vector_collection_bindings
    WHERE bot_id=requested_bot AND binding_revision=work.base_revision;
    IF NOT FOUND OR health.binding_revision IS DISTINCT FROM work.base_revision
       OR base.space_id IS DISTINCT FROM work.space_id
       OR base.dimensions IS DISTINCT FROM work.dimensions THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object('code','VECTOR_REBUILD_BASE_CHANGED',
                'message','Published vector base changed','status',409));
    END IF;
    SELECT COALESCE(current_seq,0) INTO head_seq
    FROM __forest_module_schema__.vector_ref_clocks
    WHERE bot_id=requested_bot FOR UPDATE;
    head_seq := COALESCE(head_seq,0);
    IF head_seq<>work.audit_seq OR work.replay_cursor<>work.audit_seq THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object('code','VECTOR_REBUILD_HEAD_MOVED',
                'message','Reference ledger moved after candidate audit','status',409));
    END IF;
    IF EXISTS (
        SELECT 1 FROM __forest_module_schema__.vector_io_tickets
        WHERE bot_id=requested_bot AND released_at IS NULL
          AND expires_at>clock_timestamp()
    ) THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object('code','VECTOR_REBUILD_DRAIN_PENDING',
                'message','Old physical calls have not drained','status',409));
    END IF;
    INSERT INTO __forest_module_schema__.vector_collection_bindings(
        bot_id,binding_revision,space_id,collection_name,collection_id,
        dimensions,metric
    ) VALUES(requested_bot,work.candidate_revision,work.space_id,
             work.candidate_name,work.candidate_id,work.dimensions,base.metric);
    UPDATE __forest_module_schema__.vector_collection_health
    SET binding_revision=work.candidate_revision,state='READY',reason=NULL,
        observed_at=clock_timestamp()
    WHERE bot_id=requested_bot AND binding_revision=work.base_revision;
    UPDATE __forest_module_schema__.vector_rebuild_work
    SET state='PUBLISHED',published_at=clock_timestamp(),published_seq=head_seq,
        updated_at=clock_timestamp()
    WHERE bot_id=requested_bot AND request_id=work.request_id;
    RETURN jsonb_build_object('protocol','forest.domain-result/1',
        'ok',true,'value',receipt);
END;
$$;

CREATE FUNCTION vector_settle_rebuild_publish(context jsonb, input jsonb)
RETURNS jsonb LANGUAGE plpgsql VOLATILE SET search_path=pg_catalog AS $$
DECLARE
    requested_bot text := context->>'bot_id';
    prior __forest_module_schema__.vector_rebuild_publish_settlements%ROWTYPE;
    work __forest_module_schema__.vector_rebuild_work%ROWTYPE;
    head_seq bigint;
    receipt jsonb;
    response jsonb;
    terminal_failure boolean;
BEGIN
    IF input->>'protocol' IS DISTINCT FROM 'forest.owned-work-settle-request/1'
       OR input#>>'{outcome,status}' IS NULL
       OR input#>>'{outcome,status}' NOT IN ('SUCCEEDED','FAILED') THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object('code','VECTOR_REBUILD_PUBLISH_SETTLE_INVALID',
                'message','Publish settlement is invalid','status',422));
    END IF;
    PERFORM pg_advisory_xact_lock(hashtextextended('vector-rebuild:'||requested_bot,0));
    PERFORM pg_advisory_xact_lock(hashtextextended('vector-collection:'||requested_bot,0));
    SELECT * INTO prior FROM __forest_module_schema__.vector_rebuild_publish_settlements
    WHERE bot_id=requested_bot AND claim_token=input->>'claim_token';
    IF FOUND THEN
        IF prior.input_payload=input THEN RETURN prior.result_payload; END IF;
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object('code','VECTOR_REBUILD_PUBLISH_SETTLE_CONFLICT',
                'message','Publish claim was settled differently','status',409));
    END IF;
    SELECT * INTO work FROM __forest_module_schema__.vector_rebuild_work
    WHERE bot_id=requested_bot AND request_id=input->>'work_id' FOR UPDATE;
    IF NOT FOUND OR work.claim_token IS DISTINCT FROM input->>'claim_token'
       OR work.publish_attempt_count IS DISTINCT FROM (input->>'attempt')::bigint
       OR work.state NOT IN ('PUBLISHING','PUBLISHED') THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object('code','VECTOR_REBUILD_PUBLISH_CLAIM_STALE',
                'message','Publish claim is no longer current','status',409));
    END IF;
    receipt := input#>'{outcome,result}';
    IF input#>>'{outcome,status}'='SUCCEEDED' THEN
        IF work.state<>'PUBLISHED'
           OR receipt->>'request_id' IS DISTINCT FROM work.request_id
           OR (receipt->>'binding_revision')::bigint IS DISTINCT FROM work.candidate_revision
           OR (receipt->>'collection_id')::bigint IS DISTINCT FROM work.candidate_id
           OR (receipt->>'published_seq')::bigint IS DISTINCT FROM work.published_seq THEN
            RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
                'error',jsonb_build_object('code','VECTOR_REBUILD_PUBLISH_SETTLE_INVALID',
                    'message','Publish result differs from committed binding','status',422));
        END IF;
        UPDATE __forest_module_schema__.vector_rebuild_work
        SET claim_token=NULL,lease_deadline=NULL,claim_payload=NULL,
            updated_at=clock_timestamp()
        WHERE bot_id=requested_bot AND request_id=work.request_id;
    ELSE
        IF work.state='PUBLISHED' OR work.lease_deadline<=clock_timestamp() THEN
            RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
                'error',jsonb_build_object('code','VECTOR_REBUILD_PUBLISH_CLAIM_STALE',
                    'message','Committed publish cannot be failed','status',409));
        END IF;
        SELECT COALESCE(current_seq,0) INTO head_seq
        FROM __forest_module_schema__.vector_ref_clocks
        WHERE bot_id=requested_bot;
        head_seq := COALESCE(head_seq,0);
        terminal_failure := COALESCE(input#>>'{outcome,error,code}' IN (
            'VECTOR_REBUILD_PHYSICAL_MISMATCH',
            'VECTOR_INDEX_BINDING_STALE',
            'VECTOR_INDEX_CANDIDATE_UNAVAILABLE',
            'VECTOR_INDEX_REBUILD_REQUIRED',
            'VECTOR_REBUILD_BASE_CHANGED'),false);
        UPDATE __forest_module_schema__.vector_rebuild_work
        SET state=CASE WHEN terminal_failure THEN 'FAILED'
                       WHEN head_seq=work.audit_seq THEN 'AUDITED'
                       ELSE 'CANDIDATE_READY' END,
            audit_seq=CASE WHEN NOT terminal_failure AND head_seq=work.audit_seq
                           THEN audit_seq END,
            audit_after_ref=CASE WHEN NOT terminal_failure
                                     AND head_seq=work.audit_seq
                                 THEN audit_after_ref END,
            audited_at=CASE WHEN NOT terminal_failure AND head_seq=work.audit_seq
                            THEN audited_at END,
            claim_token=NULL,lease_deadline=NULL,claim_payload=NULL,
            error_code=COALESCE(input#>>'{outcome,error,code}',
                                'VECTOR_REBUILD_PUBLISH_FAILED'),
            updated_at=clock_timestamp()
        WHERE bot_id=requested_bot AND request_id=work.request_id;
    END IF;
    response := jsonb_build_object('protocol','forest.domain-result/1','ok',true,
        'value',jsonb_build_object('protocol','forest.owned-work-settled/1',
            'work_id',work.request_id));
    INSERT INTO __forest_module_schema__.vector_rebuild_publish_settlements(
        bot_id,claim_token,input_payload,result_payload
    ) VALUES(requested_bot,input->>'claim_token',input,response);
    RETURN response;
END;
$$;
