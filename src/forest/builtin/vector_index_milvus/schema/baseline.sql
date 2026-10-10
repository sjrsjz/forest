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
       AND ROW(NEW.purpose,NEW.input_digest,NEW.replay_locator,NEW.vector_digest)
           IS NOT DISTINCT FROM
           ROW(OLD.purpose,OLD.input_digest,OLD.replay_locator,OLD.vector_digest)
       AND NEW.published_at IS NOT NULL
       AND NEW.tombstoned_at IS NULL AND NEW.next_sweep_at IS NULL
       AND NEW.sweep_count=0 THEN
        RETURN NEW;
    END IF;
    IF OLD.state IN ('PENDING','ACTIVE') AND NEW.state='TOMBSTONED'
       AND NEW.purpose IS NULL AND NEW.input_digest IS NULL
       AND NEW.replay_locator IS NULL
       AND NEW.vector_digest IS NULL AND NEW.published_at IS NULL
       AND NEW.tombstoned_at IS NOT NULL
       AND NEW.next_sweep_at IS NOT NULL AND NEW.sweep_count=0
       AND NEW.sweep_claim_token IS NULL
       AND NEW.sweep_lease_deadline IS NULL THEN
        RETURN NEW;
    END IF;
    IF OLD.state='TOMBSTONED' AND NEW.state='TOMBSTONED'
       AND ROW(NEW.purpose,NEW.input_digest,NEW.replay_locator,
               NEW.vector_digest,NEW.tombstoned_at) IS NOT DISTINCT FROM
           ROW(OLD.purpose,OLD.input_digest,OLD.replay_locator,
               OLD.vector_digest,OLD.tombstoned_at)
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
    work_key := 'vector-sweep:' ||
        encode(sha256(convert_to(requested_token,'UTF8')),'hex');
    RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',true,
        'value',jsonb_build_object(
            'protocol','forest.owned-work-claim/1',
            'work_id',work_key,'claim_token',requested_token,'attempt',1,
            'operation_id',work_key,
            'causal_root_kind','vector.tombstone','causal_root_id',work_key,
            'payload',jsonb_build_object(
                'ref_ids',to_jsonb(selected_ids),'space_id',selected_space)));
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
    requested_digest text := context->>'input_digest';
    requested_replay jsonb := input->'replay';
    requested_vector_digest text := requested_replay->>'vector_digest';
    existing __forest_module_schema__.vector_index_refs%ROWTYPE;
    result_state text;
BEGIN
    IF requested_bot IS NULL
       OR requested_ref IS NULL OR octet_length(requested_ref) NOT BETWEEN 1 AND 256
       OR requested_space IS NULL OR octet_length(requested_space) NOT BETWEEN 1 AND 256
       OR requested_purpose IS NULL OR requested_purpose NOT IN ('DOCUMENT','QUERY')
       OR requested_digest IS NULL OR requested_digest !~ '^sha256:[a-f0-9]{64}$'
       OR jsonb_typeof(requested_replay) IS DISTINCT FROM 'object'
       OR requested_vector_digest IS NULL OR requested_vector_digest !~ '^sha256:[a-f0-9]{64}$' THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object('code','VECTOR_INDEX_INPUT_INVALID',
                'message','Vector ref reservation is invalid','status',422));
    END IF;
    PERFORM pg_advisory_xact_lock_shared(hashtextextended(
        'vector-collection:'||requested_bot,0));
    IF EXISTS(
        SELECT 1 FROM __forest_module_schema__.vector_collection_start_claims
        WHERE bot_id=requested_bot AND lease_deadline>clock_timestamp()
    ) THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object('code','VECTOR_INDEX_COLLECTION_STARTING',
                'message','Collection startup has not settled','status',503));
    END IF;
    IF NOT EXISTS(
        SELECT 1 FROM __forest_module_schema__.vector_collection_health
        WHERE bot_id=requested_bot AND state='READY'
    ) THEN
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
            replay_locator,vector_digest
        ) VALUES(
            requested_bot,requested_ref,requested_space,'PENDING',
            requested_purpose,requested_digest,requested_replay,
            requested_vector_digest
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
           OR existing.vector_digest IS DISTINCT FROM requested_vector_digest THEN
            RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
                'error',jsonb_build_object('code','VECTOR_INDEX_REF_CONFLICT',
                    'message','Vector ref was bound to other data','status',409));
        END IF;
        result_state := existing.state;
    END IF;
    RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',true,
        'value',jsonb_build_object('ref_id',requested_ref,'space_id',requested_space,
                                   'input_digest',requested_digest,'state',result_state));
END;
$$;

CREATE FUNCTION vector_ref_activate(context jsonb, input jsonb)
RETURNS jsonb LANGUAGE plpgsql VOLATILE SET search_path = pg_catalog AS $$
DECLARE
    requested_bot text := context->>'bot_id';
    requested_ref text := input->>'ref_id';
    requested_digest text := input->>'input_digest';
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
    ) THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object('code','VECTOR_INDEX_REBUILD_REQUIRED',
                'message','Vector collection is not ready','status',503));
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
        ELSIF existing.state<>'TOMBSTONED' THEN
            UPDATE __forest_module_schema__.vector_index_refs
            SET state='TOMBSTONED',purpose=NULL,input_digest=NULL,
                replay_locator=NULL,vector_digest=NULL,published_at=NULL,
                tombstoned_at=clock_timestamp(),next_sweep_at=clock_timestamp()
            WHERE bot_id=requested_bot AND ref_id=requested_ref;
        END IF;
    END LOOP;
    RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',true,
        'value',jsonb_build_object('ref_ids',input->'ref_ids',
                                   'space_id',requested_space));
END;
$$;

CREATE FUNCTION vector_ref_require_query(context jsonb, input jsonb)
RETURNS jsonb LANGUAGE plpgsql STABLE SET search_path = pg_catalog AS $$
DECLARE
    requested_bot text := context->>'bot_id';
    requested_ref text := input->>'query_ref_id';
    requested_space text := input->>'space_id';
BEGIN
    IF NOT EXISTS(
        SELECT 1 FROM __forest_module_schema__.vector_collection_health
        WHERE bot_id=requested_bot AND state='READY'
    ) THEN
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
                                       'space_id',requested_space));
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
    filtered jsonb;
BEGIN
    IF NOT EXISTS(
        SELECT 1 FROM __forest_module_schema__.vector_collection_health
        WHERE bot_id=requested_bot AND state='READY'
    ) THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object('code','VECTOR_INDEX_REBUILD_REQUIRED',
                'message','Vector collection is not ready','status',503));
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
