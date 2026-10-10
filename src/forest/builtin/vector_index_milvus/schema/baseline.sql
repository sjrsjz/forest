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
    sweep_claim_token text CHECK (octet_length(sweep_claim_token) BETWEEN 1 AND 256),
    sweep_lease_deadline timestamptz,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (bot_id,ref_id),
    CHECK ((state='ACTIVE') = (input_digest IS NOT NULL)),
    CHECK ((state='ACTIVE') = (purpose IS NOT NULL)),
    CHECK ((state='ACTIVE') = (replay_locator IS NOT NULL)),
    CHECK ((state='ACTIVE') = (vector_digest IS NOT NULL)),
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
