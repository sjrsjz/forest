CREATE TABLE counter_values (
    singleton boolean PRIMARY KEY DEFAULT true CHECK (singleton),
    value bigint NOT NULL,
    revision bigint NOT NULL CHECK (revision > 0)
);

CREATE TABLE counter_operations (
    operation_id text PRIMARY KEY,
    input_payload jsonb NOT NULL,
    result_payload jsonb NOT NULL
);

CREATE TABLE counter_work (
    work_id text PRIMARY KEY,
    input_payload jsonb NOT NULL,
    operation_id text NOT NULL UNIQUE,
    state text NOT NULL CHECK (state IN ('PENDING','CLAIMED','SUCCEEDED','FAILED','UNKNOWN')),
    attempt_count integer NOT NULL DEFAULT 0 CHECK (attempt_count >= 0),
    claim_token text,
    lease_deadline timestamptz,
    next_attempt_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    settlement jsonb,
    result_payload jsonb
);

CREATE TABLE counter_enqueue_operations (
    operation_id text PRIMARY KEY,
    input_payload jsonb NOT NULL,
    result_payload jsonb NOT NULL
);

CREATE TABLE counter_work_settlements (
    claim_token text PRIMARY KEY,
    input_payload jsonb NOT NULL,
    result_payload jsonb NOT NULL
);

CREATE FUNCTION counter_current(context jsonb, input jsonb)
RETURNS jsonb
LANGUAGE plpgsql
STABLE
SET search_path = pg_catalog
AS $$
DECLARE
    current_value bigint;
    current_revision bigint;
BEGIN
    SELECT value, revision INTO current_value, current_revision
    FROM __forest_module_schema__.counter_values
    WHERE singleton;
    RETURN jsonb_build_object(
        'protocol', 'forest.domain-result/1',
        'ok', true,
        'value', jsonb_build_object(
            'value', to_jsonb(current_value),
            'revision', COALESCE(current_revision, 0)
        )
    );
END;
$$;

CREATE FUNCTION counter_set(context jsonb, input jsonb)
RETURNS jsonb
LANGUAGE plpgsql
VOLATILE
SET search_path = pg_catalog
AS $$
DECLARE
    requested_operation text := context->>'operation_id';
    expected_revision bigint := (input->>'expected_revision')::bigint;
    requested_value bigint := (input->>'value')::bigint;
    current_revision bigint;
    existing_input jsonb;
    existing_result jsonb;
    result_value jsonb;
BEGIN
    IF requested_operation IS NULL OR octet_length(requested_operation) NOT BETWEEN 1 AND 128 THEN
        RETURN jsonb_build_object(
            'protocol', 'forest.domain-result/1', 'ok', false,
            'error', jsonb_build_object(
                'code', 'DOMAIN_OPERATION_INVALID',
                'message', 'Domain operation identity is invalid',
                'status', 422
            )
        );
    END IF;
    PERFORM pg_advisory_xact_lock(hashtextextended('forest.counter', 0));
    SELECT input_payload, result_payload INTO existing_input, existing_result
    FROM __forest_module_schema__.counter_operations
    WHERE operation_id = requested_operation;
    IF FOUND THEN
        IF existing_input = input THEN
            RETURN existing_result;
        END IF;
        RETURN jsonb_build_object(
            'protocol', 'forest.domain-result/1', 'ok', false,
            'error', jsonb_build_object(
                'code', 'DOMAIN_OPERATION_CONFLICT',
                'message', 'Domain operation was already used with another input',
                'status', 409
            )
        );
    END IF;
    SELECT revision INTO current_revision
    FROM __forest_module_schema__.counter_values
    WHERE singleton FOR UPDATE;
    current_revision := COALESCE(current_revision, 0);
    IF current_revision <> expected_revision THEN
        RETURN jsonb_build_object(
            'protocol', 'forest.domain-result/1', 'ok', false,
            'error', jsonb_build_object(
                'code', 'COUNTER_REVISION_CONFLICT',
                'message', 'Counter revision changed concurrently',
                'status', 409
            )
        );
    END IF;
    INSERT INTO __forest_module_schema__.counter_values(singleton, value, revision)
    VALUES (true, requested_value, current_revision + 1)
    ON CONFLICT (singleton) DO UPDATE
    SET value = EXCLUDED.value, revision = EXCLUDED.revision;
    result_value := jsonb_build_object(
        'protocol', 'forest.domain-result/1',
        'ok', true,
        'value', jsonb_build_object(
            'value', requested_value,
            'revision', current_revision + 1
        )
    );
    INSERT INTO __forest_module_schema__.counter_operations(
        operation_id, input_payload, result_payload
    ) VALUES (requested_operation, input, result_value);
    RETURN result_value;
END;
$$;

CREATE FUNCTION counter_set_in_transaction(context jsonb, input jsonb)
RETURNS jsonb
LANGUAGE plpgsql
VOLATILE
SET search_path = pg_catalog
AS $$
BEGIN
    RETURN __forest_module_schema__.counter_set(context, input);
END;
$$;

CREATE FUNCTION counter_set_from_plan(context jsonb, input jsonb)
RETURNS jsonb
LANGUAGE plpgsql
VOLATILE
SET search_path = pg_catalog
AS $$
BEGIN
    IF input->>'protocol' IS DISTINCT FROM 'forest.uow-step/1'
       OR jsonb_typeof(input->'input') IS DISTINCT FROM 'object'
       OR jsonb_typeof(input->'prior') IS DISTINCT FROM 'object' THEN
        RETURN jsonb_build_object(
            'protocol', 'forest.domain-result/1', 'ok', false,
            'error', jsonb_build_object(
                'code', 'COUNTER_UOW_INPUT_INVALID',
                'message', 'Counter UnitOfWork envelope is invalid',
                'status', 422
            )
        );
    END IF;
    RETURN __forest_module_schema__.counter_set(context, input->'input');
END;
$$;

CREATE FUNCTION counter_reject_plan(context jsonb, input jsonb)
RETURNS jsonb
LANGUAGE plpgsql
VOLATILE
SET search_path = pg_catalog
AS $$
BEGIN
    RETURN jsonb_build_object(
        'protocol', 'forest.domain-result/1', 'ok', false,
        'error', jsonb_build_object(
            'code', 'COUNTER_UOW_REJECTED',
            'message', 'Counter rollback probe rejected the transaction',
            'status', 409
        )
    );
END;
$$;

CREATE FUNCTION counter_enqueue(context jsonb, input jsonb)
RETURNS jsonb
LANGUAGE plpgsql
VOLATILE
SET search_path = pg_catalog
AS $$
DECLARE
    requested_operation text := context->>'operation_id';
    requested_work_id text := input->>'work_id';
    work_payload jsonb := input - 'work_id';
    existing_input jsonb;
    existing_result jsonb;
    result_value jsonb;
BEGIN
    IF requested_operation IS NULL OR octet_length(requested_operation) NOT BETWEEN 1 AND 128
       OR requested_work_id IS NULL OR octet_length(requested_work_id) NOT BETWEEN 1 AND 128 THEN
        RETURN jsonb_build_object(
            'protocol', 'forest.domain-result/1', 'ok', false,
            'error', jsonb_build_object(
                'code', 'COUNTER_WORK_INPUT_INVALID',
                'message', 'Deferred counter work identity is invalid',
                'status', 422
            )
        );
    END IF;
    SELECT input_payload, result_payload INTO existing_input, existing_result
    FROM __forest_module_schema__.counter_enqueue_operations
    WHERE operation_id = requested_operation;
    IF FOUND THEN
        IF existing_input = input THEN
            RETURN existing_result;
        END IF;
        RETURN jsonb_build_object(
            'protocol', 'forest.domain-result/1', 'ok', false,
            'error', jsonb_build_object(
                'code', 'DOMAIN_OPERATION_CONFLICT',
                'message', 'Enqueue operation was reused with another input',
                'status', 409
            )
        );
    END IF;
    IF EXISTS (
        SELECT 1 FROM __forest_module_schema__.counter_work WHERE work_id = requested_work_id
    ) THEN
        RETURN jsonb_build_object(
            'protocol', 'forest.domain-result/1', 'ok', false,
            'error', jsonb_build_object(
                'code', 'COUNTER_WORK_CONFLICT',
                'message', 'Deferred counter work identity already exists',
                'status', 409
            )
        );
    END IF;
    INSERT INTO __forest_module_schema__.counter_work(
        work_id, input_payload, operation_id, state
    ) VALUES (
        requested_work_id,
        work_payload,
        'counter-work:' || requested_work_id,
        'PENDING'
    );
    result_value := jsonb_build_object(
        'protocol', 'forest.domain-result/1',
        'ok', true,
        'value', jsonb_build_object('work_id', requested_work_id, 'state', 'PENDING')
    );
    INSERT INTO __forest_module_schema__.counter_enqueue_operations(
        operation_id, input_payload, result_payload
    ) VALUES (requested_operation, input, result_value);
    RETURN result_value;
END;
$$;

CREATE FUNCTION counter_claim_work(context jsonb, input jsonb)
RETURNS jsonb
LANGUAGE plpgsql
VOLATILE
SET search_path = pg_catalog
AS $$
DECLARE
    requested_token text := input->>'claim_token';
    requested_lease integer := (input->>'lease_seconds')::integer;
    work __forest_module_schema__.counter_work%ROWTYPE;
BEGIN
    IF input->>'protocol' IS DISTINCT FROM 'forest.owned-work-claim-request/1'
       OR requested_token IS NULL OR octet_length(requested_token) NOT BETWEEN 1 AND 256
       OR requested_lease IS NULL OR requested_lease NOT BETWEEN 3 AND 86400 THEN
        RETURN jsonb_build_object(
            'protocol', 'forest.domain-result/1', 'ok', false,
            'error', jsonb_build_object(
                'code', 'COUNTER_CLAIM_INPUT_INVALID',
                'message', 'Counter work claim request is invalid',
                'status', 422
            )
        );
    END IF;
    SELECT * INTO work FROM __forest_module_schema__.counter_work
    WHERE claim_token = requested_token;
    IF NOT FOUND THEN
        UPDATE __forest_module_schema__.counter_work
        SET state='PENDING', claim_token=NULL, lease_deadline=NULL,
            next_attempt_at=clock_timestamp()
        WHERE state='CLAIMED' AND lease_deadline <= clock_timestamp();
        SELECT * INTO work FROM __forest_module_schema__.counter_work
        WHERE state='PENDING' AND next_attempt_at <= clock_timestamp()
        ORDER BY next_attempt_at, work_id
        FOR UPDATE SKIP LOCKED LIMIT 1;
        IF NOT FOUND THEN
            RETURN jsonb_build_object(
                'protocol', 'forest.domain-result/1', 'ok', true, 'value', NULL
            );
        END IF;
        UPDATE __forest_module_schema__.counter_work
        SET state='CLAIMED', attempt_count=attempt_count+1,
            claim_token=requested_token,
            lease_deadline=clock_timestamp()+(requested_lease * interval '1 second')
        WHERE work_id=work.work_id
        RETURNING * INTO work;
    END IF;
    RETURN jsonb_build_object(
        'protocol', 'forest.domain-result/1',
        'ok', true,
        'value', jsonb_build_object(
            'protocol', 'forest.owned-work-claim/1',
            'work_id', work.work_id,
            'claim_token', work.claim_token,
            'attempt', work.attempt_count,
            'operation_id', work.operation_id,
            'causal_root_kind', 'counter.work',
            'causal_root_id', work.work_id,
            'payload', work.input_payload
        )
    );
END;
$$;

CREATE FUNCTION counter_settle_work(context jsonb, input jsonb)
RETURNS jsonb
LANGUAGE plpgsql
VOLATILE
SET search_path = pg_catalog
AS $$
DECLARE
    requested_work_id text := input->>'work_id';
    requested_token text := input->>'claim_token';
    requested_attempt integer := (input->>'attempt')::integer;
    requested_outcome jsonb := input->'outcome';
    existing_input jsonb;
    existing_result jsonb;
    result_value jsonb;
    work __forest_module_schema__.counter_work%ROWTYPE;
    terminal_state text;
BEGIN
    IF input->>'protocol' IS DISTINCT FROM 'forest.owned-work-settle-request/1'
       OR requested_work_id IS NULL OR requested_token IS NULL
       OR requested_attempt IS NULL OR requested_attempt < 1
       OR requested_outcome IS NULL
       OR requested_outcome->>'status' IS NULL
       OR requested_outcome->>'status' NOT IN ('SUCCEEDED','FAILED') THEN
        RETURN jsonb_build_object(
            'protocol', 'forest.domain-result/1', 'ok', false,
            'error', jsonb_build_object(
                'code', 'COUNTER_SETTLEMENT_INPUT_INVALID',
                'message', 'Counter work settlement request is invalid',
                'status', 422
            )
        );
    END IF;
    SELECT input_payload, result_payload INTO existing_input, existing_result
    FROM __forest_module_schema__.counter_work_settlements
    WHERE claim_token = requested_token;
    IF FOUND THEN
        IF existing_input = input THEN
            RETURN existing_result;
        END IF;
        RETURN jsonb_build_object(
            'protocol', 'forest.domain-result/1', 'ok', false,
            'error', jsonb_build_object(
                'code', 'COUNTER_SETTLEMENT_CONFLICT',
                'message', 'Counter claim token was settled with another outcome',
                'status', 409
            )
        );
    END IF;
    SELECT * INTO work FROM __forest_module_schema__.counter_work
    WHERE work_id=requested_work_id FOR UPDATE;
    IF NOT FOUND OR work.state <> 'CLAIMED'
       OR work.claim_token <> requested_token
       OR work.attempt_count <> requested_attempt THEN
        RETURN jsonb_build_object(
            'protocol', 'forest.domain-result/1', 'ok', false,
            'error', jsonb_build_object(
                'code', 'COUNTER_CLAIM_STALE',
                'message', 'Counter work claim no longer owns this item',
                'status', 409
            )
        );
    END IF;
    IF requested_outcome->>'status' = 'SUCCEEDED' THEN
        terminal_state := 'SUCCEEDED';
        UPDATE __forest_module_schema__.counter_work
        SET state=terminal_state, lease_deadline=NULL, settlement=requested_outcome,
            result_payload=requested_outcome->'result'
        WHERE work_id=requested_work_id;
    ELSIF COALESCE((requested_outcome->'error'->>'unknown')::boolean, false) THEN
        terminal_state := 'UNKNOWN';
        UPDATE __forest_module_schema__.counter_work
        SET state=terminal_state, lease_deadline=NULL, settlement=requested_outcome
        WHERE work_id=requested_work_id;
    ELSIF COALESCE((requested_outcome->'error'->>'retryable')::boolean, false)
          AND requested_attempt < 3 THEN
        terminal_state := 'PENDING';
        UPDATE __forest_module_schema__.counter_work
        SET state=terminal_state, lease_deadline=NULL, settlement=requested_outcome,
            next_attempt_at=clock_timestamp()+interval '1 second'
        WHERE work_id=requested_work_id;
    ELSE
        terminal_state := 'FAILED';
        UPDATE __forest_module_schema__.counter_work
        SET state=terminal_state, lease_deadline=NULL, settlement=requested_outcome
        WHERE work_id=requested_work_id;
    END IF;
    result_value := jsonb_build_object(
        'protocol', 'forest.domain-result/1',
        'ok', true,
        'value', jsonb_build_object(
            'protocol', 'forest.owned-work-settled/1',
            'work_id', requested_work_id
        )
    );
    INSERT INTO __forest_module_schema__.counter_work_settlements(
        claim_token, input_payload, result_payload
    ) VALUES (requested_token, input, result_value);
    RETURN result_value;
END;
$$;
