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
