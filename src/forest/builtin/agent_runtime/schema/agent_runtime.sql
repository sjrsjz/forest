CREATE TABLE agent_sessions (
    bot_id text NOT NULL REFERENCES forest_kernel.bot_identity(bot_id),
    session_id text NOT NULL CHECK (octet_length(session_id) BETWEEN 1 AND 256),
    conversation_ref text NOT NULL CHECK (octet_length(conversation_ref) BETWEEN 1 AND 256),
    start_operation_id text NOT NULL
        CHECK (octet_length(start_operation_id) BETWEEN 1 AND 256),
    input_digest text NOT NULL CHECK (input_digest ~ '^sha256:[a-f0-9]{64}$'),
    state text NOT NULL DEFAULT 'ACTIVE' CHECK (
        state IN ('ACTIVE','CLOSED','FAILED','CANCELLED')
    ),
    active_run_id text,
    session_epoch bigint NOT NULL DEFAULT 1 CHECK (session_epoch > 0),
    terminal_reason_code text CHECK (
        terminal_reason_code IS NULL OR octet_length(terminal_reason_code) BETWEEN 1 AND 128
    ),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    closed_at timestamptz,
    PRIMARY KEY (bot_id, session_id),
    UNIQUE (bot_id, start_operation_id),
    CHECK (
        (state='ACTIVE' AND active_run_id IS NOT NULL AND terminal_reason_code IS NULL
            AND closed_at IS NULL)
        OR (state='CLOSED' AND active_run_id IS NULL AND terminal_reason_code IS NULL
            AND closed_at IS NOT NULL)
        OR (state IN ('FAILED','CANCELLED') AND active_run_id IS NULL
            AND terminal_reason_code IS NOT NULL AND closed_at IS NOT NULL)
    )
);

CREATE UNIQUE INDEX one_active_agent_session
    ON agent_sessions(bot_id,conversation_ref) WHERE state='ACTIVE';

CREATE TABLE agent_runs (
    bot_id text NOT NULL,
    run_id text NOT NULL CHECK (octet_length(run_id) BETWEEN 1 AND 256),
    session_id text NOT NULL,
    start_operation_id text NOT NULL
        CHECK (octet_length(start_operation_id) BETWEEN 1 AND 256),
    input_digest text NOT NULL CHECK (input_digest ~ '^sha256:[a-f0-9]{64}$'),
    state text NOT NULL CHECK (
        state IN ('WAIT_INPUT','PREPARING_MODEL','MODEL_PENDING','PARSING','REPAIR_READY',
                  'WAIT_TOOL','WAIT_APPROVAL','BLOCKED_UNKNOWN','SAFE_BOUNDARY','FINAL_READY',
                  'RELOAD_WAIT','COMPLETED','FAILED','CANCELLED')
    ),
    agent_spec_revision text NOT NULL
        CHECK (octet_length(agent_spec_revision) BETWEEN 1 AND 256),
    config_payload_id text NOT NULL CHECK (octet_length(config_payload_id) BETWEEN 1 AND 256),
    config_payload_digest text NOT NULL CHECK (config_payload_digest ~ '^sha256:[a-f0-9]{64}$'),
    config_owner_component_id text NOT NULL
        CHECK (octet_length(config_owner_component_id) BETWEEN 1 AND 128),
    config_owner_kind text NOT NULL CHECK (octet_length(config_owner_kind) BETWEEN 1 AND 128),
    config_owner_id text NOT NULL CHECK (octet_length(config_owner_id) BETWEEN 1 AND 512),
    preset_revision text NOT NULL CHECK (octet_length(preset_revision) BETWEEN 1 AND 256),
    preset_payload_id text NOT NULL CHECK (octet_length(preset_payload_id) BETWEEN 1 AND 256),
    preset_payload_digest text NOT NULL CHECK (preset_payload_digest ~ '^sha256:[a-f0-9]{64}$'),
    preset_owner_component_id text NOT NULL
        CHECK (octet_length(preset_owner_component_id) BETWEEN 1 AND 128),
    preset_owner_kind text NOT NULL CHECK (octet_length(preset_owner_kind) BETWEEN 1 AND 128),
    preset_owner_id text NOT NULL CHECK (octet_length(preset_owner_id) BETWEEN 1 AND 512),
    core_protocol_id text NOT NULL CHECK (octet_length(core_protocol_id) BETWEEN 1 AND 128),
    core_protocol_version integer NOT NULL CHECK (core_protocol_version > 0),
    core_prompt_digest text NOT NULL CHECK (core_prompt_digest ~ '^sha256:[a-f0-9]{64}$'),
    parser_revision text NOT NULL CHECK (octet_length(parser_revision) BETWEEN 1 AND 128),
    absolute_deadline timestamptz NOT NULL,
    input_revision bigint NOT NULL CHECK (input_revision > 0),
    selected_input_end_seq bigint NOT NULL CHECK (selected_input_end_seq >= 0),
    context_revision bigint NOT NULL CHECK (context_revision > 0),
    context_view_revision bigint NOT NULL CHECK (context_view_revision >= 0),
    next_step_seq integer NOT NULL DEFAULT 1 CHECK (next_step_seq > 0),
    checkpoint_seq bigint NOT NULL DEFAULT 0 CHECK (checkpoint_seq >= 0),
    repair_count integer NOT NULL DEFAULT 0 CHECK (repair_count >= 0),
    tool_count integer NOT NULL DEFAULT 0 CHECK (tool_count >= 0),
    thinking_count integer NOT NULL DEFAULT 0 CHECK (thinking_count >= 0),
    owner_id text CHECK (owner_id IS NULL OR octet_length(owner_id) BETWEEN 1 AND 256),
    owner_epoch bigint NOT NULL DEFAULT 0 CHECK (owner_epoch >= 0),
    owner_expires_at timestamptz,
    current_model_request_id text,
    current_tool_call_id text,
    reload_operation_id text CHECK (
        reload_operation_id IS NULL OR octet_length(reload_operation_id) BETWEEN 1 AND 256
    ),
    reload_resume_phase text CHECK (
        reload_resume_phase IS NULL
        OR reload_resume_phase IN (
            'PREPARING_MODEL','REPAIR_READY','WAIT_TOOL','WAIT_APPROVAL','SAFE_BOUNDARY'
        )
    ),
    reload_generation_id text CHECK (
        reload_generation_id IS NULL OR octet_length(reload_generation_id) BETWEEN 1 AND 256
    ),
    final_candidate_payload_id text,
    final_candidate_payload_digest text CHECK (
        final_candidate_payload_digest IS NULL
        OR final_candidate_payload_digest ~ '^sha256:[a-f0-9]{64}$'
    ),
    error_code text CHECK (error_code IS NULL OR octet_length(error_code) BETWEEN 1 AND 128),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    terminal_at timestamptz,
    PRIMARY KEY (bot_id, run_id),
    UNIQUE (bot_id, start_operation_id),
    FOREIGN KEY (bot_id,session_id) REFERENCES agent_sessions(bot_id,session_id),
    CHECK (
        (owner_id IS NULL AND owner_expires_at IS NULL)
        OR (owner_id IS NOT NULL AND owner_expires_at IS NOT NULL AND owner_epoch > 0)
    ),
    CHECK (
        (final_candidate_payload_id IS NULL) = (final_candidate_payload_digest IS NULL)
    ),
    CHECK (
        (state='RELOAD_WAIT') = (reload_operation_id IS NOT NULL)
        AND (reload_operation_id IS NULL) = (reload_resume_phase IS NULL)
        AND (reload_operation_id IS NULL) = (reload_generation_id IS NULL)
    ),
    CHECK (
        (state IN ('COMPLETED','FAILED','CANCELLED') AND terminal_at IS NOT NULL)
        OR (state NOT IN ('COMPLETED','FAILED','CANCELLED') AND terminal_at IS NULL)
    ),
    CHECK (state NOT IN ('FAILED','CANCELLED') OR error_code IS NOT NULL)
);

CREATE UNIQUE INDEX one_unfinished_agent_run
    ON agent_runs(bot_id,session_id)
    WHERE state NOT IN ('COMPLETED','FAILED','CANCELLED');
CREATE INDEX agent_run_recovery_idx
    ON agent_runs(bot_id,state,owner_expires_at,updated_at);

CREATE TABLE run_cancel_requests (
    bot_id text NOT NULL,
    operation_id text NOT NULL CHECK (octet_length(operation_id) BETWEEN 1 AND 256),
    input_digest text NOT NULL CHECK (input_digest ~ '^sha256:[a-f0-9]{64}$'),
    run_id text NOT NULL,
    expected_checkpoint_seq bigint NOT NULL CHECK (expected_checkpoint_seq >= 0),
    requested_by text NOT NULL CHECK (octet_length(requested_by) BETWEEN 1 AND 256),
    reason_code text NOT NULL CHECK (octet_length(reason_code) BETWEEN 1 AND 128),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (bot_id,operation_id),
    UNIQUE (bot_id,run_id),
    FOREIGN KEY (bot_id,run_id) REFERENCES agent_runs(bot_id,run_id)
);

CREATE TABLE session_close_operations (
    bot_id text NOT NULL,
    operation_id text NOT NULL CHECK (octet_length(operation_id) BETWEEN 1 AND 256),
    input_digest text NOT NULL CHECK (input_digest ~ '^sha256:[a-f0-9]{64}$'),
    session_id text NOT NULL,
    completed_run_id text NOT NULL,
    closed_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (bot_id,operation_id),
    UNIQUE (bot_id,session_id),
    FOREIGN KEY (bot_id,session_id) REFERENCES agent_sessions(bot_id,session_id),
    FOREIGN KEY (bot_id,completed_run_id) REFERENCES agent_runs(bot_id,run_id)
);

CREATE TABLE run_inputs (
    bot_id text NOT NULL,
    run_id text NOT NULL,
    input_revision bigint NOT NULL CHECK (input_revision > 0),
    input_batch_ref text NOT NULL CHECK (octet_length(input_batch_ref) BETWEEN 1 AND 256),
    input_batch_digest text NOT NULL CHECK (input_batch_digest ~ '^sha256:[a-f0-9]{64}$'),
    selected_input_end_seq bigint NOT NULL CHECK (selected_input_end_seq >= 0),
    context_revision bigint NOT NULL CHECK (context_revision > 0),
    context_view_revision bigint NOT NULL CHECK (context_view_revision >= 0),
    source_manifest_digest text NOT NULL CHECK (source_manifest_digest ~ '^sha256:[a-f0-9]{64}$'),
    trigger_claim_ref text NOT NULL CHECK (octet_length(trigger_claim_ref) BETWEEN 1 AND 256),
    trigger_claim_digest text NOT NULL CHECK (
        trigger_claim_digest ~ '^sha256:[a-f0-9]{64}$'
    ),
    trigger_count integer NOT NULL CHECK (trigger_count > 0),
    last_trigger_ref text NOT NULL CHECK (octet_length(last_trigger_ref) BETWEEN 1 AND 256),
    last_trigger_ingress_seq bigint NOT NULL CHECK (last_trigger_ingress_seq > 0),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (bot_id,run_id,input_revision),
    UNIQUE (bot_id,input_batch_ref),
    FOREIGN KEY (bot_id,run_id) REFERENCES agent_runs(bot_id,run_id),
    CHECK (last_trigger_ingress_seq<=selected_input_end_seq)
);

CREATE TABLE run_checkpoints (
    bot_id text NOT NULL,
    run_id text NOT NULL,
    checkpoint_seq bigint NOT NULL CHECK (checkpoint_seq > 0),
    checkpoint_id text NOT NULL CHECK (octet_length(checkpoint_id) BETWEEN 1 AND 256),
    input_digest text NOT NULL CHECK (input_digest ~ '^sha256:[a-f0-9]{64}$'),
    phase text NOT NULL CHECK (
        phase IN ('WAIT_INPUT','PREPARING_MODEL','MODEL_PENDING','PARSING','REPAIR_READY',
                  'WAIT_TOOL','WAIT_APPROVAL','BLOCKED_UNKNOWN','SAFE_BOUNDARY','FINAL_READY',
                  'RELOAD_WAIT','COMPLETED','FAILED','CANCELLED')
    ),
    input_revision bigint NOT NULL CHECK (input_revision > 0),
    next_step_seq integer NOT NULL CHECK (next_step_seq > 0),
    state_payload_id text NOT NULL CHECK (octet_length(state_payload_id) BETWEEN 1 AND 256),
    state_payload_digest text NOT NULL CHECK (state_payload_digest ~ '^sha256:[a-f0-9]{64}$'),
    state_owner_component_id text NOT NULL
        CHECK (octet_length(state_owner_component_id) BETWEEN 1 AND 128),
    state_owner_kind text NOT NULL CHECK (octet_length(state_owner_kind) BETWEEN 1 AND 128),
    state_owner_id text NOT NULL CHECK (octet_length(state_owner_id) BETWEEN 1 AND 512),
    checkpoint_checksum text NOT NULL CHECK (checkpoint_checksum ~ '^sha256:[a-f0-9]{64}$'),
    owner_epoch bigint NOT NULL CHECK (owner_epoch > 0),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (bot_id,run_id,checkpoint_seq),
    UNIQUE (bot_id,checkpoint_id),
    FOREIGN KEY (bot_id,run_id) REFERENCES agent_runs(bot_id,run_id)
);

CREATE TABLE model_requests (
    bot_id text NOT NULL,
    request_id text NOT NULL CHECK (octet_length(request_id) BETWEEN 1 AND 256),
    run_id text NOT NULL,
    step_seq integer NOT NULL CHECK (step_seq > 0),
    request_kind text NOT NULL CHECK (request_kind IN ('PRIMARY','CONTINUATION','REPAIR')),
    semantic_request_digest text NOT NULL CHECK (semantic_request_digest ~ '^sha256:[a-f0-9]{64}$'),
    input_revision bigint NOT NULL CHECK (input_revision > 0),
    context_revision bigint NOT NULL CHECK (context_revision > 0),
    context_view_revision bigint NOT NULL CHECK (context_view_revision >= 0),
    core_protocol_version integer NOT NULL CHECK (core_protocol_version > 0),
    core_prompt_digest text NOT NULL CHECK (core_prompt_digest ~ '^sha256:[a-f0-9]{64}$'),
    preset_revision text NOT NULL CHECK (octet_length(preset_revision) BETWEEN 1 AND 256),
    preset_payload_digest text NOT NULL CHECK (preset_payload_digest ~ '^sha256:[a-f0-9]{64}$'),
    provider_component_id text NOT NULL
        CHECK (octet_length(provider_component_id) BETWEEN 1 AND 128),
    provider_generation_id text NOT NULL
        CHECK (octet_length(provider_generation_id) BETWEEN 1 AND 256),
    provider_profile_ref text NOT NULL
        CHECK (octet_length(provider_profile_ref) BETWEEN 1 AND 256),
    model_ref text NOT NULL CHECK (octet_length(model_ref) BETWEEN 1 AND 256),
    instruction_channel text NOT NULL CHECK (
        instruction_channel IN ('SYSTEM','DEVELOPER','INSTRUCTIONS','DEGRADED_USER')
    ),
    capability_digest text NOT NULL CHECK (capability_digest ~ '^sha256:[a-f0-9]{64}$'),
    projection_payload_id text NOT NULL
        CHECK (octet_length(projection_payload_id) BETWEEN 1 AND 256),
    projection_payload_digest text NOT NULL
        CHECK (projection_payload_digest ~ '^sha256:[a-f0-9]{64}$'),
    projection_owner_component_id text NOT NULL
        CHECK (octet_length(projection_owner_component_id) BETWEEN 1 AND 128),
    projection_owner_kind text NOT NULL
        CHECK (octet_length(projection_owner_kind) BETWEEN 1 AND 128),
    projection_owner_id text NOT NULL
        CHECK (octet_length(projection_owner_id) BETWEEN 1 AND 512),
    projected_content_digest text NOT NULL
        CHECK (projected_content_digest ~ '^sha256:[a-f0-9]{64}$'),
    source_manifest_digest text NOT NULL CHECK (source_manifest_digest ~ '^sha256:[a-f0-9]{64}$'),
    learning_candidates jsonb NOT NULL CHECK (
        jsonb_typeof(learning_candidates)='array'
        AND jsonb_array_length(learning_candidates)<=64
    ),
    output_token_budget integer NOT NULL CHECK (output_token_budget > 0),
    state text NOT NULL DEFAULT 'READY' CHECK (
        state IN ('READY','ATTEMPT_PREPARED','DISPATCHING','SUCCEEDED',
                  'NOT_APPLIED','UNKNOWN','ABANDONED')
    ),
    current_attempt_id text,
    attempt_count integer NOT NULL DEFAULT 0 CHECK (attempt_count >= 0),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (bot_id,request_id),
    UNIQUE (bot_id,run_id,step_seq,request_kind),
    FOREIGN KEY (bot_id,run_id) REFERENCES agent_runs(bot_id,run_id),
    CHECK (
        (state IN ('READY','ABANDONED') AND current_attempt_id IS NULL)
        OR (state NOT IN ('READY','ABANDONED') AND current_attempt_id IS NOT NULL)
    )
);

CREATE TABLE model_transport_attempts (
    bot_id text NOT NULL,
    attempt_id text NOT NULL CHECK (octet_length(attempt_id) BETWEEN 1 AND 256),
    request_id text NOT NULL,
    attempt_no integer NOT NULL CHECK (attempt_no > 0),
    effect_operation_ref text NOT NULL
        CHECK (octet_length(effect_operation_ref) BETWEEN 1 AND 256),
    wire_payload_id text NOT NULL CHECK (octet_length(wire_payload_id) BETWEEN 1 AND 256),
    wire_payload_digest text NOT NULL CHECK (wire_payload_digest ~ '^sha256:[a-f0-9]{64}$'),
    wire_owner_component_id text NOT NULL
        CHECK (octet_length(wire_owner_component_id) BETWEEN 1 AND 128),
    wire_owner_kind text NOT NULL CHECK (octet_length(wire_owner_kind) BETWEEN 1 AND 128),
    wire_owner_id text NOT NULL CHECK (octet_length(wire_owner_id) BETWEEN 1 AND 512),
    state text NOT NULL DEFAULT 'PREPARED' CHECK (
        state IN ('PREPARED','DISPATCHING','SUCCEEDED','NOT_APPLIED','UNKNOWN','ABANDONED')
    ),
    result_payload_id text,
    result_payload_digest text CHECK (
        result_payload_digest IS NULL OR result_payload_digest ~ '^sha256:[a-f0-9]{64}$'
    ),
    usage_payload_id text,
    usage_payload_digest text CHECK (
        usage_payload_digest IS NULL OR usage_payload_digest ~ '^sha256:[a-f0-9]{64}$'
    ),
    prepared_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    started_at timestamptz,
    settled_at timestamptz,
    resolved_at timestamptz,
    PRIMARY KEY (bot_id,attempt_id),
    UNIQUE (bot_id,request_id,attempt_no),
    UNIQUE (bot_id,effect_operation_ref),
    FOREIGN KEY (bot_id,request_id) REFERENCES model_requests(bot_id,request_id),
    CHECK ((result_payload_id IS NULL) = (result_payload_digest IS NULL)),
    CHECK ((usage_payload_id IS NULL) = (usage_payload_digest IS NULL))
);

CREATE TABLE model_observations (
    bot_id text NOT NULL,
    observation_operation_id text NOT NULL
        CHECK (octet_length(observation_operation_id) BETWEEN 1 AND 256),
    attempt_id text NOT NULL,
    input_digest text NOT NULL CHECK (input_digest ~ '^sha256:[a-f0-9]{64}$'),
    evidence_kind text NOT NULL CHECK (evidence_kind IN ('EXECUTOR','RECONCILIATION')),
    observed_state text NOT NULL CHECK (
        observed_state IN ('SUCCEEDED','NOT_APPLIED','UNKNOWN')
    ),
    effect_observation_ref text NOT NULL
        CHECK (octet_length(effect_observation_ref) BETWEEN 1 AND 256),
    result_payload_id text,
    result_payload_digest text CHECK (
        result_payload_digest IS NULL OR result_payload_digest ~ '^sha256:[a-f0-9]{64}$'
    ),
    usage_payload_id text,
    usage_payload_digest text CHECK (
        usage_payload_digest IS NULL OR usage_payload_digest ~ '^sha256:[a-f0-9]{64}$'
    ),
    accepted boolean NOT NULL,
    previous_state text NOT NULL,
    resulting_state text NOT NULL,
    conflict_code text,
    observed_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (bot_id,observation_operation_id),
    FOREIGN KEY (bot_id,attempt_id) REFERENCES model_transport_attempts(bot_id,attempt_id),
    CHECK ((result_payload_id IS NULL) = (result_payload_digest IS NULL)),
    CHECK ((usage_payload_id IS NULL) = (usage_payload_digest IS NULL)),
    CHECK (previous_state IN ('DISPATCHING','SUCCEEDED','NOT_APPLIED','UNKNOWN')),
    CHECK (resulting_state IN ('SUCCEEDED','NOT_APPLIED','UNKNOWN')),
    CHECK ((accepted AND conflict_code IS NULL) OR (NOT accepted AND conflict_code IS NOT NULL))
);

CREATE TABLE protocol_failures (
    bot_id text NOT NULL,
    failure_id text NOT NULL CHECK (octet_length(failure_id) BETWEEN 1 AND 256),
    input_digest text NOT NULL CHECK (input_digest ~ '^sha256:[a-f0-9]{64}$'),
    run_id text NOT NULL,
    failed_request_id text NOT NULL,
    failed_attempt_id text NOT NULL,
    error_code text NOT NULL CHECK (octet_length(error_code) BETWEEN 1 AND 128),
    candidate_digest text NOT NULL CHECK (candidate_digest ~ '^sha256:[a-f0-9]{64}$'),
    feedback_payload_id text NOT NULL
        CHECK (octet_length(feedback_payload_id) BETWEEN 1 AND 256),
    feedback_payload_digest text NOT NULL
        CHECK (feedback_payload_digest ~ '^sha256:[a-f0-9]{64}$'),
    feedback_owner_component_id text NOT NULL
        CHECK (octet_length(feedback_owner_component_id) BETWEEN 1 AND 128),
    feedback_owner_kind text NOT NULL
        CHECK (octet_length(feedback_owner_kind) BETWEEN 1 AND 128),
    feedback_owner_id text NOT NULL CHECK (octet_length(feedback_owner_id) BETWEEN 1 AND 512),
    resulting_state text NOT NULL CHECK (resulting_state IN ('REPAIR_READY','FAILED')),
    repair_step_seq integer CHECK (repair_step_seq IS NULL OR repair_step_seq > 0),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (bot_id,failure_id),
    UNIQUE (bot_id,failed_request_id),
    UNIQUE (bot_id,run_id,repair_step_seq),
    FOREIGN KEY (bot_id,run_id) REFERENCES agent_runs(bot_id,run_id),
    FOREIGN KEY (bot_id,failed_request_id) REFERENCES model_requests(bot_id,request_id),
    FOREIGN KEY (bot_id,failed_attempt_id) REFERENCES model_transport_attempts(bot_id,attempt_id),
    CHECK (
        (resulting_state='REPAIR_READY' AND repair_step_seq IS NOT NULL)
        OR (resulting_state='FAILED' AND repair_step_seq IS NULL)
    )
);

CREATE TABLE model_turns (
    bot_id text NOT NULL,
    turn_id text NOT NULL CHECK (octet_length(turn_id) BETWEEN 1 AND 256),
    run_id text NOT NULL,
    request_id text NOT NULL,
    step_seq integer NOT NULL CHECK (step_seq > 0),
    protocol_version integer NOT NULL CHECK (protocol_version > 0),
    raw_candidate_digest text NOT NULL CHECK (raw_candidate_digest ~ '^sha256:[a-f0-9]{64}$'),
    turn_payload_id text NOT NULL CHECK (octet_length(turn_payload_id) BETWEEN 1 AND 256),
    turn_payload_digest text NOT NULL CHECK (turn_payload_digest ~ '^sha256:[a-f0-9]{64}$'),
    turn_owner_component_id text NOT NULL
        CHECK (octet_length(turn_owner_component_id) BETWEEN 1 AND 128),
    turn_owner_kind text NOT NULL CHECK (octet_length(turn_owner_kind) BETWEEN 1 AND 128),
    turn_owner_id text NOT NULL CHECK (octet_length(turn_owner_id) BETWEEN 1 AND 512),
    assistant_payload_id text,
    assistant_payload_digest text CHECK (
        assistant_payload_digest IS NULL OR assistant_payload_digest ~ '^sha256:[a-f0-9]{64}$'
    ),
    function_call_payload_id text,
    function_call_payload_digest text CHECK (
        function_call_payload_digest IS NULL
        OR function_call_payload_digest ~ '^sha256:[a-f0-9]{64}$'
    ),
    terminal boolean NOT NULL,
    thinking_only boolean NOT NULL,
    truncated_after_function_call boolean NOT NULL,
    repair_of_request_id text,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (bot_id,turn_id),
    UNIQUE (bot_id,request_id),
    UNIQUE (bot_id,run_id,step_seq),
    FOREIGN KEY (bot_id,run_id) REFERENCES agent_runs(bot_id,run_id),
    FOREIGN KEY (bot_id,request_id) REFERENCES model_requests(bot_id,request_id),
    CHECK ((assistant_payload_id IS NULL) = (assistant_payload_digest IS NULL)),
    CHECK ((function_call_payload_id IS NULL) = (function_call_payload_digest IS NULL)),
    CHECK (NOT (terminal AND function_call_payload_id IS NOT NULL)),
    CHECK (NOT (thinking_only AND (assistant_payload_id IS NOT NULL OR function_call_payload_id IS NOT NULL)))
);

CREATE TABLE tool_calls (
    bot_id text NOT NULL,
    tool_call_id text NOT NULL CHECK (octet_length(tool_call_id) BETWEEN 1 AND 256),
    run_id text NOT NULL,
    turn_id text NOT NULL,
    step_seq integer NOT NULL CHECK (step_seq > 0),
    tool_name text NOT NULL CHECK (octet_length(tool_name) BETWEEN 1 AND 128),
    tool_component_id text NOT NULL CHECK (octet_length(tool_component_id) BETWEEN 1 AND 128),
    tool_generation_id text NOT NULL CHECK (octet_length(tool_generation_id) BETWEEN 1 AND 256),
    tool_service_id text NOT NULL CHECK (octet_length(tool_service_id) BETWEEN 1 AND 256),
    arguments_payload_id text NOT NULL
        CHECK (octet_length(arguments_payload_id) BETWEEN 1 AND 256),
    arguments_payload_digest text NOT NULL
        CHECK (arguments_payload_digest ~ '^sha256:[a-f0-9]{64}$'),
    authority_trigger_ref text NOT NULL
        CHECK (octet_length(authority_trigger_ref) BETWEEN 1 AND 256),
    authority_principal_ref text NOT NULL
        CHECK (octet_length(authority_principal_ref) BETWEEN 1 AND 256),
    tool_contract_digest text NOT NULL CHECK (tool_contract_digest ~ '^sha256:[a-f0-9]{64}$'),
    tool_binding_digest text NOT NULL CHECK (tool_binding_digest ~ '^sha256:[a-f0-9]{64}$'),
    approval_required boolean NOT NULL,
    supports_query boolean NOT NULL,
    effect_operation_ref text NOT NULL
        CHECK (octet_length(effect_operation_ref) BETWEEN 1 AND 256),
    effect_attempt_id text,
    authorization_ticket_id text,
    wire_payload_id text,
    wire_payload_digest text CHECK (
        wire_payload_digest IS NULL OR wire_payload_digest ~ '^sha256:[a-f0-9]{64}$'
    ),
    approval_ref text,
    state text NOT NULL DEFAULT 'PROPOSED' CHECK (
        state IN ('PROPOSED','WAIT_APPROVAL','DISPATCHING','SUCCEEDED','FAILED',
                  'CANCELLED','UNKNOWN')
    ),
    result_payload_id text,
    result_payload_digest text CHECK (
        result_payload_digest IS NULL OR result_payload_digest ~ '^sha256:[a-f0-9]{64}$'
    ),
    error_code text CHECK (error_code IS NULL OR octet_length(error_code) BETWEEN 1 AND 128),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (bot_id,tool_call_id),
    UNIQUE (bot_id,turn_id),
    UNIQUE (bot_id,effect_operation_ref),
    FOREIGN KEY (bot_id,run_id) REFERENCES agent_runs(bot_id,run_id),
    FOREIGN KEY (bot_id,turn_id) REFERENCES model_turns(bot_id,turn_id),
    CHECK ((result_payload_id IS NULL) = (result_payload_digest IS NULL)),
    CHECK (
        (effect_attempt_id IS NULL AND authorization_ticket_id IS NULL
            AND wire_payload_id IS NULL AND wire_payload_digest IS NULL)
        OR
        (effect_attempt_id IS NOT NULL AND authorization_ticket_id IS NOT NULL
            AND wire_payload_id IS NOT NULL AND wire_payload_digest IS NOT NULL)
    ),
    CHECK (state NOT IN ('FAILED','CANCELLED') OR error_code IS NOT NULL)
);

CREATE TABLE tool_observations (
    bot_id text NOT NULL,
    observation_operation_id text NOT NULL
        CHECK (octet_length(observation_operation_id) BETWEEN 1 AND 256),
    tool_call_id text NOT NULL,
    input_digest text NOT NULL CHECK (input_digest ~ '^sha256:[a-f0-9]{64}$'),
    observed_state text NOT NULL CHECK (
        observed_state IN ('WAIT_APPROVAL','DISPATCHING','SUCCEEDED','FAILED','CANCELLED','UNKNOWN')
    ),
    evidence_kind text NOT NULL CHECK (
        evidence_kind IN ('AUTHORIZATION','EXECUTOR','RECONCILIATION')
    ),
    effect_observation_ref text,
    approval_ref text,
    result_payload_id text,
    result_payload_digest text CHECK (
        result_payload_digest IS NULL OR result_payload_digest ~ '^sha256:[a-f0-9]{64}$'
    ),
    error_code text CHECK (error_code IS NULL OR octet_length(error_code) BETWEEN 1 AND 128),
    accepted boolean NOT NULL,
    previous_state text NOT NULL,
    resulting_state text NOT NULL,
    conflict_code text,
    observed_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (bot_id,observation_operation_id),
    FOREIGN KEY (bot_id,tool_call_id) REFERENCES tool_calls(bot_id,tool_call_id),
    CHECK ((result_payload_id IS NULL) = (result_payload_digest IS NULL)),
    CHECK ((accepted AND conflict_code IS NULL) OR (NOT accepted AND conflict_code IS NOT NULL))
);

CREATE INDEX model_request_state_idx ON model_requests(bot_id,state,updated_at);
CREATE INDEX model_attempt_state_idx ON model_transport_attempts(bot_id,state,settled_at);
CREATE INDEX tool_call_state_idx ON tool_calls(bot_id,state,updated_at);
CREATE INDEX tool_call_run_idx ON tool_calls(bot_id,run_id);
CREATE UNIQUE INDEX tool_call_approval_ref_idx ON tool_calls(bot_id,approval_ref)
    WHERE approval_ref IS NOT NULL;

CREATE FUNCTION guard_agent_session() RETURNS trigger LANGUAGE plpgsql
SET search_path FROM CURRENT AS $$
BEGIN
    IF TG_OP='DELETE' OR
       ROW(NEW.bot_id,NEW.session_id,NEW.conversation_ref,NEW.start_operation_id,
           NEW.input_digest,NEW.created_at)
       IS DISTINCT FROM
       ROW(OLD.bot_id,OLD.session_id,OLD.conversation_ref,OLD.start_operation_id,
           OLD.input_digest,OLD.created_at) THEN
        RAISE EXCEPTION 'Agent Session identity is immutable' USING ERRCODE='55000';
    END IF;
    IF NEW.state=OLD.state OR
       (OLD.state='ACTIVE' AND NEW.state IN ('CLOSED','FAILED','CANCELLED')) THEN
        RETURN NEW;
    END IF;
    RAISE EXCEPTION 'Illegal Agent Session transition' USING ERRCODE='55000';
END;
$$;
CREATE TRIGGER agent_sessions_one_way BEFORE UPDATE OR DELETE ON agent_sessions
    FOR EACH ROW EXECUTE FUNCTION guard_agent_session();

CREATE FUNCTION guard_agent_run() RETURNS trigger LANGUAGE plpgsql
SET search_path FROM CURRENT AS $$
BEGIN
    IF TG_OP='DELETE' OR
       ROW(NEW.bot_id,NEW.run_id,NEW.session_id,NEW.start_operation_id,NEW.input_digest,
           NEW.agent_spec_revision,NEW.config_payload_id,NEW.config_payload_digest,
           NEW.config_owner_component_id,NEW.config_owner_kind,NEW.config_owner_id,
           NEW.preset_revision,NEW.preset_payload_id,NEW.preset_payload_digest,
           NEW.preset_owner_component_id,NEW.preset_owner_kind,NEW.preset_owner_id,
           NEW.core_protocol_id,NEW.core_protocol_version,NEW.core_prompt_digest,
           NEW.parser_revision,NEW.absolute_deadline,NEW.created_at)
       IS DISTINCT FROM
       ROW(OLD.bot_id,OLD.run_id,OLD.session_id,OLD.start_operation_id,OLD.input_digest,
           OLD.agent_spec_revision,OLD.config_payload_id,OLD.config_payload_digest,
           OLD.config_owner_component_id,OLD.config_owner_kind,OLD.config_owner_id,
           OLD.preset_revision,OLD.preset_payload_id,OLD.preset_payload_digest,
           OLD.preset_owner_component_id,OLD.preset_owner_kind,OLD.preset_owner_id,
           OLD.core_protocol_id,OLD.core_protocol_version,OLD.core_prompt_digest,
           OLD.parser_revision,OLD.absolute_deadline,OLD.created_at) THEN
        RAISE EXCEPTION 'Agent Run identity is immutable' USING ERRCODE='55000';
    END IF;
    IF NEW.input_revision < OLD.input_revision OR
       NEW.selected_input_end_seq < OLD.selected_input_end_seq OR
       NEW.context_revision < OLD.context_revision OR
       NEW.context_view_revision < OLD.context_view_revision OR
       NEW.next_step_seq < OLD.next_step_seq OR NEW.checkpoint_seq < OLD.checkpoint_seq OR
       NEW.repair_count < OLD.repair_count OR NEW.tool_count < OLD.tool_count OR
       NEW.thinking_count < OLD.thinking_count OR NEW.owner_epoch < OLD.owner_epoch THEN
        RAISE EXCEPTION 'Agent Run monotonic state regressed' USING ERRCODE='55000';
    END IF;
    IF NEW.state=OLD.state OR
       (OLD.state='WAIT_INPUT' AND NEW.state IN ('PREPARING_MODEL','FAILED','CANCELLED')) OR
       (OLD.state='PREPARING_MODEL' AND NEW.state IN ('MODEL_PENDING','FAILED','CANCELLED','RELOAD_WAIT')) OR
       (OLD.state='MODEL_PENDING' AND NEW.state IN ('PARSING','PREPARING_MODEL','BLOCKED_UNKNOWN','FAILED','CANCELLED')) OR
       (OLD.state='PARSING' AND NEW.state IN ('PREPARING_MODEL','REPAIR_READY','WAIT_TOOL','SAFE_BOUNDARY','FAILED','CANCELLED')) OR
       (OLD.state='REPAIR_READY' AND NEW.state IN ('PREPARING_MODEL','FAILED','CANCELLED','RELOAD_WAIT')) OR
       (OLD.state='WAIT_TOOL' AND NEW.state IN ('PREPARING_MODEL','WAIT_APPROVAL','BLOCKED_UNKNOWN','FAILED','CANCELLED','RELOAD_WAIT')) OR
       (OLD.state='WAIT_APPROVAL' AND NEW.state IN ('PREPARING_MODEL','WAIT_TOOL','FAILED','CANCELLED','RELOAD_WAIT')) OR
       (OLD.state='BLOCKED_UNKNOWN' AND NEW.state IN ('MODEL_PENDING','PARSING','PREPARING_MODEL','WAIT_TOOL','FAILED','CANCELLED')) OR
       (OLD.state='SAFE_BOUNDARY' AND NEW.state IN ('PREPARING_MODEL','FINAL_READY','FAILED','CANCELLED','RELOAD_WAIT')) OR
       (OLD.state='FINAL_READY' AND NEW.state IN ('COMPLETED','PREPARING_MODEL','FAILED','CANCELLED')) OR
       (OLD.state='RELOAD_WAIT' AND NEW.state IN (
           'PREPARING_MODEL','REPAIR_READY','WAIT_TOOL','WAIT_APPROVAL','SAFE_BOUNDARY',
           'FAILED','CANCELLED'
       )) THEN
        RETURN NEW;
    END IF;
    RAISE EXCEPTION 'Illegal Agent Run transition' USING ERRCODE='55000';
END;
$$;
CREATE TRIGGER agent_runs_one_way BEFORE UPDATE OR DELETE ON agent_runs
    FOR EACH ROW EXECUTE FUNCTION guard_agent_run();

CREATE FUNCTION reject_agent_fact_change() RETURNS trigger LANGUAGE plpgsql
SET search_path FROM CURRENT AS $$
BEGIN
    RAISE EXCEPTION 'Agent fact is immutable' USING ERRCODE='55000';
END;
$$;
CREATE TRIGGER run_inputs_immutable BEFORE UPDATE OR DELETE ON run_inputs
    FOR EACH ROW EXECUTE FUNCTION reject_agent_fact_change();
CREATE TRIGGER run_checkpoints_immutable BEFORE UPDATE OR DELETE ON run_checkpoints
    FOR EACH ROW EXECUTE FUNCTION reject_agent_fact_change();
CREATE TRIGGER run_cancel_requests_immutable BEFORE UPDATE OR DELETE ON run_cancel_requests
    FOR EACH ROW EXECUTE FUNCTION reject_agent_fact_change();
CREATE TRIGGER model_turns_immutable BEFORE UPDATE OR DELETE ON model_turns
    FOR EACH ROW EXECUTE FUNCTION reject_agent_fact_change();
CREATE TRIGGER tool_observations_immutable BEFORE UPDATE OR DELETE ON tool_observations
    FOR EACH ROW EXECUTE FUNCTION reject_agent_fact_change();

CREATE TABLE agent_reload_resume_settlements (
    claim_token text PRIMARY KEY CHECK (octet_length(claim_token) BETWEEN 1 AND 256),
    input_payload jsonb NOT NULL,
    result_payload jsonb NOT NULL,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp()
);
CREATE TRIGGER agent_reload_resume_settlements_immutable
    BEFORE UPDATE OR DELETE ON agent_reload_resume_settlements
    FOR EACH ROW EXECUTE FUNCTION reject_agent_fact_change();

CREATE FUNCTION guard_model_request() RETURNS trigger LANGUAGE plpgsql
SET search_path FROM CURRENT AS $$
BEGIN
    IF TG_OP='DELETE' OR
       ROW(NEW.bot_id,NEW.request_id,NEW.run_id,NEW.step_seq,NEW.request_kind,
           NEW.semantic_request_digest,NEW.input_revision,NEW.context_revision,
           NEW.context_view_revision,NEW.core_protocol_version,NEW.core_prompt_digest,
           NEW.preset_revision,NEW.preset_payload_digest,NEW.provider_component_id,
           NEW.provider_generation_id,NEW.provider_profile_ref,NEW.model_ref,
           NEW.instruction_channel,NEW.capability_digest,NEW.projection_payload_id,
           NEW.projection_payload_digest,NEW.projection_owner_component_id,
           NEW.projection_owner_kind,NEW.projection_owner_id,NEW.projected_content_digest,
           NEW.source_manifest_digest,NEW.learning_candidates,
           NEW.output_token_budget,NEW.created_at)
       IS DISTINCT FROM
       ROW(OLD.bot_id,OLD.request_id,OLD.run_id,OLD.step_seq,OLD.request_kind,
           OLD.semantic_request_digest,OLD.input_revision,OLD.context_revision,
           OLD.context_view_revision,OLD.core_protocol_version,OLD.core_prompt_digest,
           OLD.preset_revision,OLD.preset_payload_digest,OLD.provider_component_id,
           OLD.provider_generation_id,OLD.provider_profile_ref,OLD.model_ref,
           OLD.instruction_channel,OLD.capability_digest,OLD.projection_payload_id,
           OLD.projection_payload_digest,OLD.projection_owner_component_id,
           OLD.projection_owner_kind,OLD.projection_owner_id,OLD.projected_content_digest,
           OLD.source_manifest_digest,OLD.learning_candidates,
           OLD.output_token_budget,OLD.created_at) THEN
        RAISE EXCEPTION 'Model request identity is immutable' USING ERRCODE='55000';
    END IF;
    IF ((OLD.state IN ('READY','NOT_APPLIED') AND NEW.state='ATTEMPT_PREPARED') AND
        NEW.attempt_count=OLD.attempt_count+1 AND NEW.current_attempt_id IS NOT NULL AND EXISTS (
            SELECT 1 FROM model_transport_attempts a
            WHERE a.bot_id=NEW.bot_id AND a.request_id=NEW.request_id
              AND a.attempt_id=NEW.current_attempt_id AND a.attempt_no=NEW.attempt_count
              AND a.state='PREPARED'
        )) THEN
        RETURN NEW;
    END IF;
    IF NEW.attempt_count<>OLD.attempt_count THEN
        RAISE EXCEPTION 'Model request attempt count changed outside prepare'
            USING ERRCODE='55000';
    END IF;
    IF (OLD.state='ATTEMPT_PREPARED' AND NEW.state='DISPATCHING') AND EXISTS (
        SELECT 1 FROM model_transport_attempts a
        WHERE a.bot_id=NEW.bot_id AND a.attempt_id=NEW.current_attempt_id
          AND a.request_id=NEW.request_id AND a.state='DISPATCHING'
    ) THEN
        RETURN NEW;
    END IF;
    IF ((OLD.state='DISPATCHING' AND NEW.state IN ('SUCCEEDED','NOT_APPLIED','UNKNOWN')) OR
        (OLD.state='UNKNOWN' AND NEW.state IN ('SUCCEEDED','NOT_APPLIED'))) AND EXISTS (
        SELECT 1 FROM model_transport_attempts a
        WHERE a.bot_id=NEW.bot_id AND a.attempt_id=NEW.current_attempt_id
          AND a.request_id=NEW.request_id AND a.state=NEW.state
    ) THEN
        RETURN NEW;
    END IF;
    IF OLD.state='ATTEMPT_PREPARED' AND NEW.state IN ('READY','ABANDONED') THEN
        RETURN NEW;
    END IF;
    RAISE EXCEPTION 'Illegal model request transition' USING ERRCODE='55000';
END;
$$;
CREATE TRIGGER model_requests_one_way BEFORE UPDATE OR DELETE ON model_requests
    FOR EACH ROW EXECUTE FUNCTION guard_model_request();

CREATE FUNCTION guard_model_attempt() RETURNS trigger LANGUAGE plpgsql
SET search_path FROM CURRENT AS $$
BEGIN
    IF TG_OP='DELETE' OR
       ROW(NEW.bot_id,NEW.attempt_id,NEW.request_id,NEW.attempt_no,
           NEW.effect_operation_ref,NEW.wire_payload_id,NEW.wire_payload_digest,
           NEW.wire_owner_component_id,NEW.wire_owner_kind,NEW.wire_owner_id,
           NEW.prepared_at)
       IS DISTINCT FROM
       ROW(OLD.bot_id,OLD.attempt_id,OLD.request_id,OLD.attempt_no,
           OLD.effect_operation_ref,OLD.wire_payload_id,OLD.wire_payload_digest,
           OLD.wire_owner_component_id,OLD.wire_owner_kind,OLD.wire_owner_id,
           OLD.prepared_at) THEN
        RAISE EXCEPTION 'Model attempt identity is immutable' USING ERRCODE='55000';
    END IF;
    IF OLD.state='PREPARED' AND NEW.state IN ('DISPATCHING','ABANDONED') THEN
        RETURN NEW;
    END IF;
    IF ((OLD.state='DISPATCHING' AND NEW.state IN ('SUCCEEDED','NOT_APPLIED','UNKNOWN')) OR
        (OLD.state='UNKNOWN' AND NEW.state IN ('SUCCEEDED','NOT_APPLIED'))) AND EXISTS (
            SELECT 1 FROM model_observations o
            WHERE o.bot_id=NEW.bot_id AND o.attempt_id=NEW.attempt_id
              AND o.accepted AND o.previous_state=OLD.state AND o.resulting_state=NEW.state
        ) THEN
        RETURN NEW;
    END IF;
    RAISE EXCEPTION 'Illegal model attempt transition' USING ERRCODE='55000';
END;
$$;
CREATE TRIGGER model_attempts_one_way BEFORE UPDATE OR DELETE ON model_transport_attempts
    FOR EACH ROW EXECUTE FUNCTION guard_model_attempt();

CREATE FUNCTION guard_tool_call() RETURNS trigger LANGUAGE plpgsql
SET search_path FROM CURRENT AS $$
BEGIN
    IF TG_OP='DELETE' OR
       ROW(NEW.bot_id,NEW.tool_call_id,NEW.run_id,NEW.turn_id,NEW.step_seq,
           NEW.tool_name,NEW.tool_component_id,NEW.tool_generation_id,NEW.tool_service_id,
           NEW.arguments_payload_id,NEW.arguments_payload_digest,
           NEW.authority_trigger_ref,NEW.authority_principal_ref,
           NEW.tool_contract_digest,NEW.tool_binding_digest,NEW.approval_required,
           NEW.supports_query,NEW.effect_operation_ref,NEW.created_at)
       IS DISTINCT FROM
       ROW(OLD.bot_id,OLD.tool_call_id,OLD.run_id,OLD.turn_id,OLD.step_seq,
           OLD.tool_name,OLD.tool_component_id,OLD.tool_generation_id,OLD.tool_service_id,
           OLD.arguments_payload_id,OLD.arguments_payload_digest,
           OLD.authority_trigger_ref,OLD.authority_principal_ref,
           OLD.tool_contract_digest,OLD.tool_binding_digest,OLD.approval_required,
           OLD.supports_query,OLD.effect_operation_ref,OLD.created_at) THEN
        RAISE EXCEPTION 'Tool call identity is immutable' USING ERRCODE='55000';
    END IF;
    IF ((OLD.state='PROPOSED' AND NEW.state IN ('WAIT_APPROVAL','DISPATCHING','FAILED','CANCELLED')) OR
        (OLD.state='WAIT_APPROVAL' AND NEW.state IN ('DISPATCHING','FAILED','CANCELLED')) OR
        (OLD.state='DISPATCHING' AND NEW.state IN ('SUCCEEDED','FAILED','CANCELLED','UNKNOWN')) OR
        (OLD.state='UNKNOWN' AND NEW.state IN ('SUCCEEDED','FAILED','CANCELLED'))) AND EXISTS (
            SELECT 1 FROM tool_observations o
            WHERE o.bot_id=NEW.bot_id AND o.tool_call_id=NEW.tool_call_id
              AND o.accepted AND o.previous_state=OLD.state AND o.resulting_state=NEW.state
        ) THEN
        RETURN NEW;
    END IF;
    IF OLD.state=NEW.state AND OLD.state IN ('PROPOSED','WAIT_APPROVAL')
       AND OLD.effect_attempt_id IS NULL AND NEW.effect_attempt_id IS NOT NULL
       AND NEW.authorization_ticket_id IS NOT NULL
       AND NEW.wire_payload_id IS NOT NULL AND NEW.wire_payload_digest IS NOT NULL
       AND (NEW.state='PROPOSED' OR (NEW.approval_required AND NEW.approval_ref IS NOT NULL))
       AND ROW(NEW.approval_ref,NEW.result_payload_id,NEW.result_payload_digest,NEW.error_code)
           IS NOT DISTINCT FROM
           ROW(OLD.approval_ref,OLD.result_payload_id,OLD.result_payload_digest,OLD.error_code) THEN
        RETURN NEW;
    END IF;
    RAISE EXCEPTION 'Illegal tool call transition' USING ERRCODE='55000';
END;
$$;
CREATE TRIGGER tool_calls_one_way BEFORE UPDATE OR DELETE ON tool_calls
    FOR EACH ROW EXECUTE FUNCTION guard_tool_call();
CREATE TRIGGER model_observations_immutable BEFORE UPDATE OR DELETE ON model_observations
    FOR EACH ROW EXECUTE FUNCTION reject_agent_fact_change();
CREATE TRIGGER protocol_failures_immutable BEFORE UPDATE OR DELETE ON protocol_failures
    FOR EACH ROW EXECUTE FUNCTION reject_agent_fact_change();

CREATE FUNCTION agent_run_snapshot(context jsonb, input jsonb)
RETURNS jsonb
LANGUAGE plpgsql
STABLE
SET search_path = pg_catalog
AS $$
DECLARE
    requested_bot text := context->>'bot_id';
    requested_run text := input->>'run_id';
    run_row record;
    session_row record;
    inputs_value jsonb;
    cancel_value jsonb;
    owner_value jsonb := NULL;
    final_value jsonb := NULL;
BEGIN
    SELECT * INTO run_row
    FROM __forest_module_schema__.agent_runs
    WHERE bot_id=requested_bot AND run_id=requested_run;
    IF NOT FOUND THEN
        RETURN jsonb_build_object(
            'protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object(
                'code','AGENT_RUN_NOT_FOUND','message','Agent Run does not exist','status',404
            )
        );
    END IF;
    SELECT * INTO session_row
    FROM __forest_module_schema__.agent_sessions
    WHERE bot_id=requested_bot AND session_id=run_row.session_id;
    SELECT COALESCE(jsonb_agg(jsonb_build_object(
        'input_revision',i.input_revision,'input_batch_ref',i.input_batch_ref,
        'input_batch_digest',i.input_batch_digest,
        'selected_input_end_seq',i.selected_input_end_seq,
        'context_revision',i.context_revision,'context_view_revision',i.context_view_revision,
        'source_manifest_digest',i.source_manifest_digest,
        'trigger_claim_ref',i.trigger_claim_ref,
        'trigger_claim_digest',i.trigger_claim_digest,
        'trigger_count',i.trigger_count,
        'last_trigger_ref',i.last_trigger_ref,
        'last_trigger_ingress_seq',i.last_trigger_ingress_seq,
        'created_at',i.created_at
    ) ORDER BY i.input_revision),'[]'::jsonb)
    INTO inputs_value
    FROM __forest_module_schema__.run_inputs i
    WHERE i.bot_id=requested_bot AND i.run_id=requested_run;
    SELECT CASE WHEN count(*)=0 THEN NULL ELSE jsonb_build_object(
        'operation_id',min(c.operation_id),'input_digest',min(c.input_digest),
        'expected_checkpoint_seq',min(c.expected_checkpoint_seq),
        'requested_by',min(c.requested_by),'reason_code',min(c.reason_code),
        'created_at',min(c.created_at)
    ) END
    INTO cancel_value
    FROM __forest_module_schema__.run_cancel_requests c
    WHERE c.bot_id=requested_bot AND c.run_id=requested_run;
    IF run_row.owner_id IS NOT NULL THEN
        owner_value := jsonb_build_object(
            'owner_id',run_row.owner_id,'epoch',run_row.owner_epoch,
            'expires_at',run_row.owner_expires_at
        );
    END IF;
    IF run_row.final_candidate_payload_id IS NOT NULL THEN
        final_value := jsonb_build_object(
            'payload_id',run_row.final_candidate_payload_id,
            'digest',run_row.final_candidate_payload_digest
        );
    END IF;
    RETURN jsonb_build_object(
        'protocol','forest.domain-result/1','ok',true,
        'value',jsonb_build_object(
            'run_id',run_row.run_id,'session_id',run_row.session_id,
            'conversation_ref',session_row.conversation_ref,'state',run_row.state,
            'profile',jsonb_build_object(
                'agent_spec_revision',run_row.agent_spec_revision,
                'config',jsonb_build_object(
                    'payload_id',run_row.config_payload_id,'digest',run_row.config_payload_digest,
                    'owner_component_id',run_row.config_owner_component_id,
                    'owner_kind',run_row.config_owner_kind,'owner_id',run_row.config_owner_id
                ),
                'preset_revision',run_row.preset_revision,
                'preset',jsonb_build_object(
                    'payload_id',run_row.preset_payload_id,'digest',run_row.preset_payload_digest,
                    'owner_component_id',run_row.preset_owner_component_id,
                    'owner_kind',run_row.preset_owner_kind,'owner_id',run_row.preset_owner_id
                ),
                'core_protocol_id',run_row.core_protocol_id,
                'core_protocol_version',run_row.core_protocol_version,
                'core_prompt_digest',run_row.core_prompt_digest,
                'parser_revision',run_row.parser_revision
            ),
            'absolute_deadline',run_row.absolute_deadline,
            'progress',jsonb_build_object(
                'input_revision',run_row.input_revision,
                'selected_input_end_seq',run_row.selected_input_end_seq,
                'context_revision',run_row.context_revision,
                'context_view_revision',run_row.context_view_revision,
                'next_step_seq',run_row.next_step_seq,'checkpoint_seq',run_row.checkpoint_seq,
                'repair_count',run_row.repair_count,'tool_count',run_row.tool_count,
                'thinking_count',run_row.thinking_count
            ),
            'owner',owner_value,
            'current',jsonb_build_object(
                'model_request_id',run_row.current_model_request_id,
                'tool_call_id',run_row.current_tool_call_id,
                'reload_operation_id',run_row.reload_operation_id,
                'reload_resume_phase',run_row.reload_resume_phase,
                'reload_generation_id',run_row.reload_generation_id
            ),
            'final_candidate',final_value,'error_code',run_row.error_code,
            'session',jsonb_build_object(
                'state',session_row.state,'active_run_id',session_row.active_run_id,
                'session_epoch',session_row.session_epoch,
                'terminal_reason_code',session_row.terminal_reason_code,
                'created_at',session_row.created_at,'updated_at',session_row.updated_at,
                'closed_at',session_row.closed_at
            ),
            'inputs',inputs_value,'cancel_request',cancel_value,
            'created_at',run_row.created_at,'updated_at',run_row.updated_at,
            'terminal_at',run_row.terminal_at
        )
    );
END;
$$;

CREATE FUNCTION agent_run_recovery(context jsonb, input jsonb)
RETURNS jsonb
LANGUAGE plpgsql
STABLE
SET search_path = pg_catalog
AS $$
DECLARE
    requested_bot text := context->>'bot_id';
    requested_run text := input->>'run_id';
    run_row record;
    cancel_value jsonb := NULL;
    request_value jsonb := NULL;
    request_id_value text := NULL;
    attempt_value jsonb := NULL;
    tool_value jsonb := NULL;
    turn_value jsonb := NULL;
    failure_value jsonb := NULL;
BEGIN
    SELECT * INTO run_row
    FROM __forest_module_schema__.agent_runs
    WHERE bot_id=requested_bot AND run_id=requested_run;
    IF NOT FOUND THEN
        RETURN jsonb_build_object(
            'protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object(
                'code','AGENT_RUN_NOT_FOUND','message','Agent Run does not exist','status',404
            )
        );
    END IF;

    SELECT jsonb_build_object(
        'operation_id',c.operation_id,'reason_code',c.reason_code
    ) INTO cancel_value
    FROM __forest_module_schema__.run_cancel_requests c
    WHERE c.bot_id=requested_bot AND c.run_id=requested_run;

    SELECT m.request_id, jsonb_build_object(
        'request_id',m.request_id,'step_seq',m.step_seq,'request_kind',m.request_kind,
        'provider_component_id',m.provider_component_id,'state',m.state,
        'attempt_count',m.attempt_count
    ) INTO request_id_value, request_value
    FROM __forest_module_schema__.model_requests m
    WHERE m.bot_id=requested_bot AND (
        (run_row.current_model_request_id IS NOT NULL
            AND m.request_id=run_row.current_model_request_id)
        OR
        (run_row.current_model_request_id IS NULL AND run_row.state='PREPARING_MODEL'
            AND m.run_id=requested_run AND m.step_seq=run_row.next_step_seq)
    )
    ORDER BY
        CASE WHEN m.request_id=run_row.current_model_request_id THEN 0 ELSE 1 END,
        m.request_id
    LIMIT 1;

    IF request_id_value IS NOT NULL THEN
        SELECT jsonb_build_object(
            'attempt_id',a.attempt_id,'effect_operation_ref',a.effect_operation_ref,
            'state',a.state,'result_payload_id',a.result_payload_id,
            'result_payload_digest',a.result_payload_digest
        ) INTO attempt_value
        FROM __forest_module_schema__.model_transport_attempts a
        JOIN __forest_module_schema__.model_requests m
          ON m.bot_id=a.bot_id AND m.request_id=a.request_id
        WHERE a.bot_id=requested_bot AND m.request_id=request_id_value
          AND a.attempt_id=m.current_attempt_id;
    END IF;

    IF run_row.current_tool_call_id IS NOT NULL THEN
        SELECT jsonb_build_object(
            'tool_call_id',t.tool_call_id,'tool_name',t.tool_name,
            'tool_component_id',t.tool_component_id,
            'tool_generation_id',t.tool_generation_id,'tool_service_id',t.tool_service_id,
            'arguments_payload_id',t.arguments_payload_id,
            'arguments_payload_digest',t.arguments_payload_digest,
            'authority_trigger_ref',t.authority_trigger_ref,
            'authority_principal_ref',t.authority_principal_ref,
            'tool_contract_digest',t.tool_contract_digest,
            'tool_binding_digest',t.tool_binding_digest,
            'approval_required',t.approval_required,'supports_query',t.supports_query,
            'effect_operation_ref',t.effect_operation_ref,
            'effect_attempt_id',t.effect_attempt_id,
            'authorization_ticket_id',t.authorization_ticket_id,
            'wire_payload_id',t.wire_payload_id,
            'wire_payload_digest',t.wire_payload_digest,
            'approval_ref',t.approval_ref,'state',t.state
        ) INTO tool_value
        FROM __forest_module_schema__.tool_calls t
        WHERE t.bot_id=requested_bot AND t.tool_call_id=run_row.current_tool_call_id;
    END IF;

    SELECT jsonb_build_object(
        'turn_id',t.turn_id,'request_id',t.request_id,'step_seq',t.step_seq,
        'assistant_payload_id',t.assistant_payload_id,
        'assistant_payload_digest',t.assistant_payload_digest,'terminal',t.terminal
    ) INTO turn_value
    FROM __forest_module_schema__.model_turns t
    WHERE t.bot_id=requested_bot AND t.run_id=requested_run
    ORDER BY t.step_seq DESC,t.turn_id DESC
    LIMIT 1;

    SELECT jsonb_build_object(
        'failure_id',f.failure_id,'failed_request_id',f.failed_request_id,
        'error_code',f.error_code,
        'candidate_digest',f.candidate_digest,
        'feedback_payload_id',f.feedback_payload_id,
        'feedback_payload_digest',f.feedback_payload_digest,
        'resulting_state',f.resulting_state,'repair_step_seq',f.repair_step_seq
    ) INTO failure_value
    FROM __forest_module_schema__.protocol_failures f
    WHERE f.bot_id=requested_bot AND f.run_id=requested_run
      AND f.repair_step_seq=run_row.next_step_seq
    ORDER BY f.created_at DESC,f.failure_id DESC
    LIMIT 1;

    RETURN jsonb_build_object(
        'protocol','forest.domain-result/1','ok',true,
        'value',jsonb_build_object(
            'run_id',run_row.run_id,'state',run_row.state,
            'next_step_seq',run_row.next_step_seq,'repair_count',run_row.repair_count,
            'cancel_request',cancel_value,'model_request',request_value,
            'model_attempt',attempt_value,'tool_call',tool_value,
            'latest_turn',turn_value,'protocol_failure',failure_value
        )
    );
END;
$$;

CREATE FUNCTION agent_execution_history(context jsonb, input jsonb)
RETURNS jsonb
LANGUAGE plpgsql
STABLE
SET search_path = pg_catalog
AS $$
DECLARE
    requested_bot text := context->>'bot_id';
    requested_run text := input->>'run_id';
    history_value jsonb;
BEGIN
    IF requested_run IS NULL OR octet_length(requested_run) NOT BETWEEN 1 AND 256 THEN
        RETURN jsonb_build_object(
            'protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object(
                'code','AGENT_HISTORY_INPUT_INVALID',
                'message','Agent execution history input is invalid','status',422
            )
        );
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM __forest_module_schema__.agent_runs
        WHERE bot_id=requested_bot AND run_id=requested_run
    ) THEN
        RETURN jsonb_build_object(
            'protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object(
                'code','AGENT_RUN_NOT_FOUND','message','Agent Run does not exist','status',404
            )
        );
    END IF;
    SELECT COALESCE(jsonb_agg(row_value ORDER BY step_seq,turn_id),'[]'::jsonb)
    INTO history_value
    FROM (
        SELECT t.step_seq,t.turn_id,jsonb_build_object(
            'turn_id',t.turn_id,
            'request_id',t.request_id,
            'step_seq',t.step_seq,
            'input_revision',r.input_revision,
            'provider_component_id',r.provider_component_id,
            'model_attempt_id',a.attempt_id,
            'model_result_payload_id',a.result_payload_id,
            'model_result_payload_digest',a.result_payload_digest,
            'tool_call_id',c.tool_call_id,
            'tool_name',c.tool_name,
            'tool_component_id',c.tool_component_id,
            'tool_state',c.state,
            'function_call_payload_id',t.function_call_payload_id,
            'function_call_payload_digest',t.function_call_payload_digest,
            'result_payload_id',c.result_payload_id,
            'result_payload_digest',c.result_payload_digest,
            'tool_error_code',c.error_code
        ) AS row_value
        FROM __forest_module_schema__.model_turns t
        JOIN __forest_module_schema__.model_requests r
          ON r.bot_id=t.bot_id AND r.request_id=t.request_id
        LEFT JOIN __forest_module_schema__.tool_calls c
          ON c.bot_id=t.bot_id AND c.turn_id=t.turn_id
        LEFT JOIN LATERAL (
            SELECT x.attempt_id,x.result_payload_id,x.result_payload_digest
            FROM __forest_module_schema__.model_transport_attempts x
            WHERE x.bot_id=t.bot_id AND x.request_id=t.request_id
              AND x.state='SUCCEEDED'
            ORDER BY x.attempt_no DESC LIMIT 1
        ) a ON TRUE
        WHERE t.bot_id=requested_bot AND t.run_id=requested_run
    ) history_rows;
    RETURN jsonb_build_object(
        'protocol','forest.domain-result/1','ok',true,
        'value',jsonb_build_object(
            'protocol','forest.agent-execution-history/1',
            'run_id',requested_run,
            'turns',history_value
        )
    );
END;
$$;

CREATE FUNCTION agent_acquire_run(context jsonb, envelope jsonb)
RETURNS jsonb
LANGUAGE plpgsql
VOLATILE
SET search_path = pg_catalog
AS $$
DECLARE
    body jsonb := envelope->'input';
    requested_bot text := context->>'bot_id';
    requested_run text := body->>'run_id';
    requested_owner text := body->>'owner_id';
    requested_seconds integer := (body->>'lease_seconds')::integer;
    run_row record;
    next_epoch bigint;
    next_expiry timestamptz;
BEGIN
    IF envelope->>'protocol' IS DISTINCT FROM 'forest.uow-step/1'
       OR envelope#>>'{caller,component_id}' IS DISTINCT FROM 'agent.runtime' THEN
        RETURN jsonb_build_object(
            'protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object(
                'code','AGENT_RUN_LEASE_INPUT_INVALID',
                'message','Agent Run lease envelope is invalid','status',422
            )
        );
    END IF;
    SELECT *,clock_timestamp() AS database_now INTO run_row
    FROM __forest_module_schema__.agent_runs
    WHERE bot_id=requested_bot AND run_id=requested_run FOR UPDATE;
    IF NOT FOUND THEN
        RETURN jsonb_build_object(
            'protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object(
                'code','AGENT_RUN_NOT_FOUND','message','Agent Run does not exist','status',404
            )
        );
    END IF;
    IF run_row.state IN ('COMPLETED','FAILED','CANCELLED') THEN
        RETURN jsonb_build_object(
            'protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object(
                'code','AGENT_RUN_TERMINAL','message','Agent Run is terminal','status',409
            )
        );
    END IF;
    IF run_row.owner_id IS NOT NULL AND run_row.owner_expires_at>run_row.database_now
       AND run_row.owner_id IS DISTINCT FROM requested_owner THEN
        RETURN jsonb_build_object(
            'protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object(
                'code','AGENT_RUN_BUSY','message','Agent Run lease is busy','status',409
            )
        );
    END IF;
    next_epoch := CASE
        WHEN run_row.owner_id=requested_owner AND run_row.owner_expires_at>run_row.database_now
        THEN run_row.owner_epoch ELSE run_row.owner_epoch+1 END;
    next_expiry := run_row.database_now + make_interval(secs=>requested_seconds);
    UPDATE __forest_module_schema__.agent_runs
    SET owner_id=requested_owner,owner_epoch=next_epoch,owner_expires_at=next_expiry,
        updated_at=clock_timestamp()
    WHERE bot_id=requested_bot AND run_id=requested_run;
    RETURN jsonb_build_object(
        'protocol','forest.domain-result/1','ok',true,
        'value',jsonb_build_object(
            'run_id',requested_run,'owner_id',requested_owner,
            'epoch',next_epoch,'expires_at',next_expiry
        )
    );
END;
$$;

CREATE FUNCTION agent_renew_run(context jsonb, envelope jsonb)
RETURNS jsonb
LANGUAGE plpgsql
VOLATILE
SET search_path = pg_catalog
AS $$
DECLARE
    body jsonb := envelope->'input';
    owner_value jsonb := body->'owner';
    requested_bot text := context->>'bot_id';
    requested_run text := owner_value->>'run_id';
    requested_owner text := owner_value->>'owner_id';
    requested_epoch bigint := (owner_value->>'epoch')::bigint;
    requested_seconds integer := (body->>'lease_seconds')::integer;
    run_row record;
    next_expiry timestamptz;
BEGIN
    IF envelope->>'protocol' IS DISTINCT FROM 'forest.uow-step/1'
       OR envelope#>>'{caller,component_id}' IS DISTINCT FROM 'agent.runtime' THEN
        RETURN jsonb_build_object(
            'protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object(
                'code','AGENT_RUN_LEASE_INPUT_INVALID',
                'message','Agent Run lease envelope is invalid','status',422
            )
        );
    END IF;
    SELECT *,clock_timestamp() AS database_now INTO run_row
    FROM __forest_module_schema__.agent_runs
    WHERE bot_id=requested_bot AND run_id=requested_run FOR UPDATE;
    IF NOT FOUND OR run_row.owner_id IS DISTINCT FROM requested_owner
       OR run_row.owner_epoch IS DISTINCT FROM requested_epoch
       OR run_row.owner_expires_at IS NULL
       OR run_row.owner_expires_at<=run_row.database_now THEN
        RETURN jsonb_build_object(
            'protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object(
                'code','STALE_AGENT_RUN_OWNER','message','Agent Run owner is stale','status',409
            )
        );
    END IF;
    next_expiry := run_row.database_now + make_interval(secs=>requested_seconds);
    UPDATE __forest_module_schema__.agent_runs
    SET owner_expires_at=next_expiry,updated_at=clock_timestamp()
    WHERE bot_id=requested_bot AND run_id=requested_run;
    RETURN jsonb_build_object(
        'protocol','forest.domain-result/1','ok',true,
        'value',jsonb_build_object(
            'run_id',requested_run,'owner_id',requested_owner,
            'epoch',requested_epoch,'expires_at',next_expiry
        )
    );
END;
$$;

CREATE FUNCTION agent_release_run(context jsonb, envelope jsonb)
RETURNS jsonb
LANGUAGE plpgsql
VOLATILE
SET search_path = pg_catalog
AS $$
DECLARE
    owner_value jsonb := envelope#>'{input,owner}';
    requested_bot text := context->>'bot_id';
    requested_run text := owner_value->>'run_id';
    requested_owner text := owner_value->>'owner_id';
    requested_epoch bigint := (owner_value->>'epoch')::bigint;
    run_row record;
BEGIN
    IF envelope->>'protocol' IS DISTINCT FROM 'forest.uow-step/1'
       OR envelope#>>'{caller,component_id}' IS DISTINCT FROM 'agent.runtime' THEN
        RETURN jsonb_build_object(
            'protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object(
                'code','AGENT_RUN_LEASE_INPUT_INVALID',
                'message','Agent Run lease envelope is invalid','status',422
            )
        );
    END IF;
    SELECT *,clock_timestamp() AS database_now INTO run_row
    FROM __forest_module_schema__.agent_runs
    WHERE bot_id=requested_bot AND run_id=requested_run FOR UPDATE;
    IF NOT FOUND OR run_row.owner_id IS DISTINCT FROM requested_owner
       OR run_row.owner_epoch IS DISTINCT FROM requested_epoch
       OR run_row.owner_expires_at IS NULL
       OR run_row.owner_expires_at<=run_row.database_now THEN
        RETURN jsonb_build_object(
            'protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object(
                'code','STALE_AGENT_RUN_OWNER','message','Agent Run owner is stale','status',409
            )
        );
    END IF;
    UPDATE __forest_module_schema__.agent_runs
    SET owner_id=NULL,owner_expires_at=NULL,updated_at=clock_timestamp()
    WHERE bot_id=requested_bot AND run_id=requested_run;
    RETURN jsonb_build_object(
        'protocol','forest.domain-result/1','ok',true,
        'value',jsonb_build_object('run_id',requested_run,'released',true)
    );
END;
$$;

CREATE FUNCTION agent_fail_exhausted_model(context jsonb, envelope jsonb)
RETURNS jsonb
LANGUAGE plpgsql
VOLATILE
SET search_path = pg_catalog
AS $$
DECLARE
    body jsonb := envelope->'input';
    owner_value jsonb := body->'owner';
    checkpoint_value jsonb := body->'checkpoint_state';
    requested_bot text := context->>'bot_id';
    requested_run text := owner_value->>'run_id';
    requested_owner text := owner_value->>'owner_id';
    requested_epoch bigint := (owner_value->>'epoch')::bigint;
    requested_request text := body->>'request_id';
    requested_attempt text := body->>'attempt_id';
    requested_checkpoint text := body->>'checkpoint_id';
    input_digest_value text := context->>'participant_input_digest';
    run_row record;
    session_row record;
    request_row record;
    attempt_row record;
    sequence_value bigint;
    checkpoint_digest_value text;
    checksum_value text;
BEGIN
    IF envelope->>'protocol' IS DISTINCT FROM 'forest.uow-step/1'
       OR envelope#>>'{caller,component_id}' IS DISTINCT FROM 'agent.runtime'
       OR input_digest_value !~ '^sha256:[a-f0-9]{64}$'
       OR requested_checkpoint IS NULL
       OR checkpoint_value->>'owner_component_id' IS DISTINCT FROM 'agent.runtime'
       OR checkpoint_value->>'owner_kind' IS DISTINCT FROM 'checkpoint.state'
       OR checkpoint_value->>'owner_id' IS DISTINCT FROM requested_checkpoint THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object('code','AGENT_MODEL_FAILURE_INPUT_INVALID',
                'message','Model exhaustion evidence is invalid','status',422));
    END IF;
    SELECT *,clock_timestamp() AS database_now INTO run_row
    FROM __forest_module_schema__.agent_runs
    WHERE bot_id=requested_bot AND run_id=requested_run FOR UPDATE;
    IF NOT FOUND OR run_row.owner_id IS DISTINCT FROM requested_owner
       OR run_row.owner_epoch IS DISTINCT FROM requested_epoch
       OR run_row.owner_expires_at IS NULL
       OR run_row.owner_expires_at<=run_row.database_now THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object('code','STALE_AGENT_RUN_OWNER',
                'message','Agent Run owner is stale','status',409));
    END IF;
    SELECT * INTO session_row FROM __forest_module_schema__.agent_sessions
    WHERE bot_id=requested_bot AND session_id=run_row.session_id FOR UPDATE;
    SELECT * INTO request_row FROM __forest_module_schema__.model_requests
    WHERE bot_id=requested_bot AND request_id=requested_request FOR UPDATE;
    SELECT * INTO attempt_row FROM __forest_module_schema__.model_transport_attempts
    WHERE bot_id=requested_bot AND attempt_id=requested_attempt FOR SHARE;
    IF run_row.state IS DISTINCT FROM 'PREPARING_MODEL'
       OR run_row.current_model_request_id IS DISTINCT FROM requested_request
       OR session_row.state IS DISTINCT FROM 'ACTIVE'
       OR session_row.active_run_id IS DISTINCT FROM requested_run
       OR request_row.run_id IS DISTINCT FROM requested_run
       OR request_row.step_seq IS DISTINCT FROM run_row.next_step_seq
       OR request_row.state IS DISTINCT FROM 'NOT_APPLIED'
       OR request_row.attempt_count IS DISTINCT FROM 3
       OR request_row.current_attempt_id IS DISTINCT FROM requested_attempt
       OR attempt_row.request_id IS DISTINCT FROM requested_request
       OR attempt_row.attempt_no IS DISTINCT FROM 3
       OR attempt_row.state IS DISTINCT FROM 'NOT_APPLIED'
       OR EXISTS (
           SELECT 1 FROM __forest_module_schema__.model_transport_attempts a
           WHERE a.bot_id=requested_bot AND a.request_id=requested_request
             AND a.state NOT IN ('NOT_APPLIED','ABANDONED')
       ) THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object('code','AGENT_MODEL_FAILURE_BOUNDARY_CLOSED',
                'message','Model attempts are not all known to be unsent','status',409));
    END IF;
    sequence_value := run_row.checkpoint_seq+1;
    checkpoint_digest_value := 'sha256:' || encode(sha256(convert_to(jsonb_build_object(
        'run_id',requested_run,'request_id',requested_request,
        'attempt_id',requested_attempt,'phase','FAILED',
        'input_revision',run_row.input_revision,
        'next_step_seq',run_row.next_step_seq,'state',checkpoint_value,
        'owner_epoch',requested_epoch
    )::text,'UTF8')),'hex');
    checksum_value := 'sha256:' || encode(sha256(convert_to(jsonb_build_array(
        requested_run,sequence_value,'FAILED',run_row.input_revision,
        run_row.next_step_seq,checkpoint_value->>'digest',requested_epoch
    )::text,'UTF8')),'hex');
    INSERT INTO __forest_module_schema__.run_checkpoints(
        bot_id,run_id,checkpoint_seq,checkpoint_id,input_digest,phase,input_revision,
        next_step_seq,state_payload_id,state_payload_digest,state_owner_component_id,
        state_owner_kind,state_owner_id,checkpoint_checksum,owner_epoch
    ) VALUES (
        requested_bot,requested_run,sequence_value,requested_checkpoint,
        checkpoint_digest_value,'FAILED',run_row.input_revision,run_row.next_step_seq,
        checkpoint_value->>'payload_id',checkpoint_value->>'digest',
        checkpoint_value->>'owner_component_id',checkpoint_value->>'owner_kind',
        checkpoint_value->>'owner_id',checksum_value,requested_epoch
    );
    UPDATE __forest_module_schema__.agent_runs
    SET state='FAILED',checkpoint_seq=sequence_value,current_model_request_id=NULL,
        error_code='AGENT_MODEL_TRANSPORT_FAILED',terminal_at=run_row.database_now,
        updated_at=clock_timestamp()
    WHERE bot_id=requested_bot AND run_id=requested_run;
    UPDATE __forest_module_schema__.agent_sessions
    SET state='FAILED',active_run_id=NULL,
        terminal_reason_code='AGENT_MODEL_TRANSPORT_FAILED',
        closed_at=clock_timestamp(),updated_at=clock_timestamp()
    WHERE bot_id=requested_bot AND session_id=run_row.session_id;
    RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',true,
        'value',jsonb_build_object('run_id',requested_run,'state','FAILED',
            'error_code','AGENT_MODEL_TRANSPORT_FAILED'));
END;
$$;

CREATE FUNCTION agent_prepare_model_request(context jsonb, envelope jsonb)
RETURNS jsonb
LANGUAGE plpgsql
VOLATILE
SET search_path = pg_catalog
AS $$
DECLARE
    body jsonb := envelope->'input';
    owner_value jsonb := body->'owner';
    request_value jsonb := body->'request';
    projection_value jsonb := request_value->'projection';
    requested_bot text := context->>'bot_id';
    requested_run text := owner_value->>'run_id';
    requested_owner text := owner_value->>'owner_id';
    requested_epoch bigint := (owner_value->>'epoch')::bigint;
    requested_request text := request_value->>'request_id';
    run_row record;
    existing record;
    semantic_digest_value text;
    has_repair boolean;
BEGIN
    IF envelope->>'protocol' IS DISTINCT FROM 'forest.uow-step/1'
       OR envelope#>>'{caller,component_id}' IS DISTINCT FROM 'agent.runtime'
       OR context->>'participant_input_digest' !~ '^sha256:[a-f0-9]{64}$'
       OR projection_value->>'owner_component_id' IS DISTINCT FROM 'agent.runtime'
       OR projection_value->>'owner_kind' IS DISTINCT FROM 'model.projection'
       OR projection_value->>'owner_id' IS DISTINCT FROM requested_request
       OR jsonb_typeof(request_value->'learning_candidates') IS DISTINCT FROM 'array'
       OR jsonb_array_length(request_value->'learning_candidates')>64
       OR (request_value->>'request_kind'='REPAIR'
           AND jsonb_array_length(request_value->'learning_candidates')<>0) THEN
        RETURN jsonb_build_object(
            'protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object(
                'code','AGENT_MODEL_REQUEST_INPUT_INVALID',
                'message','Model request envelope is invalid','status',422
            )
        );
    END IF;
    SELECT *,clock_timestamp() AS database_now INTO run_row
    FROM __forest_module_schema__.agent_runs
    WHERE bot_id=requested_bot AND run_id=requested_run FOR UPDATE;
    IF NOT FOUND OR run_row.owner_id IS DISTINCT FROM requested_owner
       OR run_row.owner_epoch IS DISTINCT FROM requested_epoch
       OR run_row.owner_expires_at IS NULL
       OR run_row.owner_expires_at<=run_row.database_now THEN
        RETURN jsonb_build_object(
            'protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object(
                'code','STALE_AGENT_RUN_OWNER','message','Agent Run owner is stale','status',409
            )
        );
    END IF;
    PERFORM pg_advisory_xact_lock(hashtextextended(
        'forest.agent.runtime/model-request/' || requested_bot || '/' || requested_request,0
    ));
    SELECT * INTO existing
    FROM __forest_module_schema__.model_requests
    WHERE bot_id=requested_bot AND request_id=requested_request FOR UPDATE;
    IF FOUND THEN
        IF existing.run_id IS DISTINCT FROM requested_run
           OR existing.request_kind IS DISTINCT FROM request_value->>'request_kind'
           OR existing.provider_component_id IS DISTINCT FROM request_value->>'provider_component_id'
           OR existing.provider_generation_id IS DISTINCT FROM request_value->>'provider_generation_id'
           OR existing.provider_profile_ref IS DISTINCT FROM request_value->>'provider_profile_ref'
           OR existing.model_ref IS DISTINCT FROM request_value->>'model_ref'
           OR existing.instruction_channel IS DISTINCT FROM request_value->>'instruction_channel'
           OR existing.capability_digest IS DISTINCT FROM request_value->>'capability_digest'
           OR existing.projection_payload_id IS DISTINCT FROM projection_value->>'payload_id'
           OR existing.projection_payload_digest IS DISTINCT FROM projection_value->>'digest'
           OR existing.projection_owner_component_id
                IS DISTINCT FROM projection_value->>'owner_component_id'
           OR existing.projection_owner_kind IS DISTINCT FROM projection_value->>'owner_kind'
           OR existing.projection_owner_id IS DISTINCT FROM projection_value->>'owner_id'
           OR existing.projected_content_digest
                IS DISTINCT FROM request_value->>'projected_content_digest'
           OR existing.source_manifest_digest
                IS DISTINCT FROM request_value->>'source_manifest_digest'
           OR existing.learning_candidates
                IS DISTINCT FROM request_value->'learning_candidates'
           OR existing.output_token_budget
                IS DISTINCT FROM (request_value->>'output_token_budget')::integer THEN
            RETURN jsonb_build_object(
                'protocol','forest.domain-result/1','ok',false,
                'error',jsonb_build_object(
                    'code','MODEL_REQUEST_ID_CONFLICT',
                    'message','Model request identity differs','status',409
                )
            );
        END IF;
        RETURN jsonb_build_object(
            'protocol','forest.domain-result/1','ok',true,
            'value',jsonb_build_object(
                'request_id',existing.request_id,'step_seq',existing.step_seq,
                'request_kind',existing.request_kind,'state',existing.state,
                'attempt_count',existing.attempt_count
            )
        );
    END IF;
    SELECT EXISTS (
        SELECT 1 FROM __forest_module_schema__.protocol_failures f
        WHERE f.bot_id=requested_bot AND f.run_id=requested_run
          AND f.repair_step_seq=run_row.next_step_seq
    ) INTO has_repair;
    IF run_row.state NOT IN ('PREPARING_MODEL','REPAIR_READY') THEN
        RETURN jsonb_build_object(
            'protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object(
                'code','MODEL_REQUEST_BOUNDARY_CLOSED',
                'message','Run is not preparing a model request','status',409
            )
        );
    END IF;
    IF has_repair IS DISTINCT FROM (request_value->>'request_kind'='REPAIR') THEN
        RETURN jsonb_build_object(
            'protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object(
                'code','MODEL_REQUEST_KIND_CONFLICT',
                'message','Model request kind differs from durable repair state','status',409
            )
        );
    END IF;
    semantic_digest_value := 'sha256:' || encode(sha256(convert_to(jsonb_build_object(
        'run_id',requested_run,'step_seq',run_row.next_step_seq,
        'input_revision',run_row.input_revision,'context_revision',run_row.context_revision,
        'context_view_revision',run_row.context_view_revision,
        'core_protocol_version',run_row.core_protocol_version,
        'core_prompt_digest',run_row.core_prompt_digest,
        'preset_revision',run_row.preset_revision,
        'preset_payload_digest',run_row.preset_payload_digest,
        'request',request_value
    )::text,'UTF8')),'hex');
    INSERT INTO __forest_module_schema__.model_requests(
        bot_id,request_id,run_id,step_seq,request_kind,semantic_request_digest,
        input_revision,context_revision,context_view_revision,core_protocol_version,
        core_prompt_digest,preset_revision,preset_payload_digest,provider_component_id,
        provider_generation_id,provider_profile_ref,model_ref,instruction_channel,
        capability_digest,projection_payload_id,projection_payload_digest,
        projection_owner_component_id,projection_owner_kind,projection_owner_id,
        projected_content_digest,source_manifest_digest,learning_candidates,
        output_token_budget
    ) VALUES (
        requested_bot,requested_request,requested_run,run_row.next_step_seq,
        request_value->>'request_kind',semantic_digest_value,run_row.input_revision,
        run_row.context_revision,run_row.context_view_revision,run_row.core_protocol_version,
        run_row.core_prompt_digest,run_row.preset_revision,run_row.preset_payload_digest,
        request_value->>'provider_component_id',request_value->>'provider_generation_id',
        request_value->>'provider_profile_ref',request_value->>'model_ref',
        request_value->>'instruction_channel',request_value->>'capability_digest',
        projection_value->>'payload_id',projection_value->>'digest',
        projection_value->>'owner_component_id',projection_value->>'owner_kind',
        projection_value->>'owner_id',request_value->>'projected_content_digest',
        request_value->>'source_manifest_digest',
        request_value->'learning_candidates',
        (request_value->>'output_token_budget')::integer
    );
    IF run_row.state='REPAIR_READY' THEN
        UPDATE __forest_module_schema__.agent_runs
        SET state='PREPARING_MODEL',current_model_request_id=NULL,
            updated_at=clock_timestamp()
        WHERE bot_id=requested_bot AND run_id=requested_run;
    END IF;
    RETURN jsonb_build_object(
        'protocol','forest.domain-result/1','ok',true,
        'value',jsonb_build_object(
            'request_id',requested_request,'step_seq',run_row.next_step_seq,
            'request_kind',request_value->>'request_kind','state','READY','attempt_count',0
        )
    );
END;
$$;

CREATE FUNCTION agent_prepare_model_attempt(context jsonb, envelope jsonb)
RETURNS jsonb
LANGUAGE plpgsql
VOLATILE
SET search_path = pg_catalog
AS $$
DECLARE
    body jsonb := envelope->'input';
    owner_value jsonb := body->'owner';
    effect_preparation jsonb := envelope->'prior'->(body->>'effect_step_id');
    effect_permit jsonb := effect_preparation->'permit';
    input_value jsonb := effect_preparation->'input_payload';
    wire_value jsonb := effect_preparation->'wire_payload';
    requested_bot text := context->>'bot_id';
    requested_run text := owner_value->>'run_id';
    requested_owner text := owner_value->>'owner_id';
    requested_epoch bigint := (owner_value->>'epoch')::bigint;
    requested_request text := body->>'request_id';
      requested_attempt text := body->>'attempt_id';
      requested_effect text := body->>'effect_operation_ref';
      requested_service text := body->>'provider_service_id';
    run_row record;
    request_row record;
    existing record;
    attempt_no_value integer;
BEGIN
    IF envelope->>'protocol' IS DISTINCT FROM 'forest.uow-step/1'
         OR envelope#>>'{caller,component_id}' IS DISTINCT FROM 'agent.runtime'
         OR requested_service IS DISTINCT FROM 'provider.model.dispatch'
         OR body->>'effect_step_id' IS DISTINCT FROM 'effect'
         OR effect_preparation->>'protocol'
            IS DISTINCT FROM 'forest.effect-command-permit/1'
         OR NULLIF(effect_preparation->>'authorization_ticket_id','') IS NULL
         OR effect_permit->>'protocol' IS DISTINCT FROM 'forest.effect-permit/1'
         OR effect_permit->>'effect_operation_id' IS DISTINCT FROM requested_effect
         OR effect_permit->>'attempt_id' IS DISTINCT FROM requested_attempt
         OR effect_permit->>'wire_payload_id' IS DISTINCT FROM wire_value->>'payload_id'
         OR effect_permit->>'wire_payload_digest' IS DISTINCT FROM wire_value->>'digest'
         OR input_value->>'owner_component_id' IS DISTINCT FROM 'kernel.effect'
         OR input_value->>'owner_kind' IS DISTINCT FROM 'effect.input'
         OR input_value->>'owner_id' IS DISTINCT FROM requested_effect
         OR wire_value->>'owner_component_id' IS DISTINCT FROM 'kernel.effect'
         OR wire_value->>'owner_kind' IS DISTINCT FROM 'effect.wire'
       OR wire_value->>'owner_id' IS DISTINCT FROM requested_attempt THEN
        RETURN jsonb_build_object(
            'protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object(
                'code','AGENT_MODEL_ATTEMPT_INPUT_INVALID',
                'message','Model attempt envelope is invalid','status',422
            )
        );
    END IF;
    SELECT *,clock_timestamp() AS database_now INTO run_row
    FROM __forest_module_schema__.agent_runs
    WHERE bot_id=requested_bot AND run_id=requested_run FOR UPDATE;
    IF NOT FOUND OR run_row.owner_id IS DISTINCT FROM requested_owner
       OR run_row.owner_epoch IS DISTINCT FROM requested_epoch
       OR run_row.owner_expires_at IS NULL
       OR run_row.owner_expires_at<=run_row.database_now THEN
        RETURN jsonb_build_object(
            'protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object(
                'code','STALE_AGENT_RUN_OWNER','message','Agent Run owner is stale','status',409
            )
        );
    END IF;
    PERFORM pg_advisory_xact_lock(hashtextextended(
        'forest.agent.runtime/model-attempt/' || requested_bot || '/' || requested_attempt,0
    ));
    SELECT * INTO existing
    FROM __forest_module_schema__.model_transport_attempts
    WHERE bot_id=requested_bot AND attempt_id=requested_attempt FOR UPDATE;
    IF FOUND THEN
        IF existing.request_id IS DISTINCT FROM requested_request
           OR existing.effect_operation_ref IS DISTINCT FROM requested_effect
           OR effect_permit->>'attempt_no' IS DISTINCT FROM '1'
           OR existing.wire_payload_id IS DISTINCT FROM wire_value->>'payload_id'
           OR existing.wire_payload_digest IS DISTINCT FROM wire_value->>'digest'
           OR existing.wire_owner_component_id IS DISTINCT FROM wire_value->>'owner_component_id'
           OR existing.wire_owner_kind IS DISTINCT FROM wire_value->>'owner_kind'
           OR existing.wire_owner_id IS DISTINCT FROM wire_value->>'owner_id' THEN
            RETURN jsonb_build_object(
                'protocol','forest.domain-result/1','ok',false,
                'error',jsonb_build_object(
                    'code','MODEL_ATTEMPT_ID_CONFLICT',
                    'message','Model attempt identity differs','status',409
                )
            );
        END IF;
        RETURN jsonb_build_object(
            'protocol','forest.domain-result/1','ok',true,
            'value',jsonb_build_object(
                'protocol','forest.agent-model-effect-prepared/1',
                'attempt_id',existing.attempt_id,'request_id',existing.request_id,
                'attempt_no',existing.attempt_no,
                'effect_operation_ref',existing.effect_operation_ref,'state',existing.state,
                'authorization_ticket_id',effect_preparation->>'authorization_ticket_id',
                'input_payload',input_value,'wire_payload',wire_value,'permit',effect_permit
            )
        );
    END IF;
    SELECT * INTO request_row
    FROM __forest_module_schema__.model_requests
    WHERE bot_id=requested_bot AND request_id=requested_request FOR UPDATE;
    IF NOT FOUND OR request_row.run_id IS DISTINCT FROM requested_run THEN
        RETURN jsonb_build_object(
            'protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object(
                'code','MODEL_REQUEST_NOT_FOUND','message','Model request does not exist','status',404
            )
        );
    END IF;
      IF effect_permit->>'executor_component_id'
            IS DISTINCT FROM request_row.provider_component_id
         OR effect_permit->>'executor_generation_id'
            IS DISTINCT FROM request_row.provider_generation_id
         OR effect_permit->>'fence_kind' IS DISTINCT FROM 'runtime.graph' THEN
        RETURN jsonb_build_object(
            'protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object(
                'code','AGENT_MODEL_ATTEMPT_INPUT_INVALID',
                'message','Model executor differs from the frozen provider','status',422
            )
        );
    END IF;
    IF run_row.state IS DISTINCT FROM 'PREPARING_MODEL'
       OR request_row.state NOT IN ('READY','NOT_APPLIED') THEN
        RETURN jsonb_build_object(
            'protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object(
                'code','MODEL_ATTEMPT_BOUNDARY_CLOSED',
                'message','Model request cannot prepare an attempt','status',409
            )
        );
    END IF;
    attempt_no_value := request_row.attempt_count+1;
    -- Each Model transport attempt owns a new Effect operation. Its Effect
    -- attempt counter starts at one even when the Model request is retrying.
    IF effect_permit->>'attempt_no' IS DISTINCT FROM '1' THEN
        RETURN jsonb_build_object(
            'protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object(
                'code','AGENT_MODEL_ATTEMPT_INPUT_INVALID',
                'message','Model Effect must begin at its first attempt','status',422
            )
        );
    END IF;
    INSERT INTO __forest_module_schema__.model_transport_attempts(
        bot_id,attempt_id,request_id,attempt_no,effect_operation_ref,
        wire_payload_id,wire_payload_digest,wire_owner_component_id,
        wire_owner_kind,wire_owner_id
    ) VALUES (
        requested_bot,requested_attempt,requested_request,attempt_no_value,requested_effect,
        wire_value->>'payload_id',wire_value->>'digest',wire_value->>'owner_component_id',
        wire_value->>'owner_kind',wire_value->>'owner_id'
    );
    UPDATE __forest_module_schema__.model_requests
    SET state='ATTEMPT_PREPARED',current_attempt_id=requested_attempt,
        attempt_count=attempt_no_value,updated_at=clock_timestamp()
    WHERE bot_id=requested_bot AND request_id=requested_request;
    RETURN jsonb_build_object(
        'protocol','forest.domain-result/1','ok',true,
        'value',jsonb_build_object(
            'protocol','forest.agent-model-effect-prepared/1',
            'attempt_id',requested_attempt,'request_id',requested_request,
            'attempt_no',attempt_no_value,'effect_operation_ref',requested_effect,
            'state','PREPARED',
            'authorization_ticket_id',effect_preparation->>'authorization_ticket_id',
            'input_payload',input_value,'wire_payload',wire_value,'permit',effect_permit
        )
    );
END;
$$;

CREATE FUNCTION agent_mark_model_handoff(context jsonb, envelope jsonb)
RETURNS jsonb
LANGUAGE plpgsql
VOLATILE
SET search_path = pg_catalog
AS $$
DECLARE
    body jsonb := envelope->'input';
    owner_value jsonb := body->'owner';
    requested_bot text := context->>'bot_id';
    requested_run text := owner_value->>'run_id';
    requested_owner text := owner_value->>'owner_id';
    requested_epoch bigint := (owner_value->>'epoch')::bigint;
    requested_request text := body->>'request_id';
      requested_attempt text := body->>'attempt_id';
      effect_handoff jsonb := envelope->'prior'->'effect';
    run_row record;
    request_row record;
    attempt_row record;
BEGIN
      IF envelope->>'protocol' IS DISTINCT FROM 'forest.uow-step/1'
         OR envelope#>>'{caller,component_id}' IS DISTINCT FROM 'agent.runtime'
         OR effect_handoff->>'effect_operation_id' IS NULL
         OR effect_handoff->>'attempt_id' IS DISTINCT FROM requested_attempt
         OR effect_handoff->>'state' IS DISTINCT FROM 'DISPATCHING' THEN
        RETURN jsonb_build_object(
            'protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object(
                'code','AGENT_MODEL_HANDOFF_INPUT_INVALID',
                'message','Model handoff envelope is invalid','status',422
            )
        );
    END IF;
    SELECT *,clock_timestamp() AS database_now INTO run_row
    FROM __forest_module_schema__.agent_runs
    WHERE bot_id=requested_bot AND run_id=requested_run FOR UPDATE;
    IF NOT FOUND OR run_row.owner_id IS DISTINCT FROM requested_owner
       OR run_row.owner_epoch IS DISTINCT FROM requested_epoch
       OR run_row.owner_expires_at IS NULL
       OR run_row.owner_expires_at<=run_row.database_now THEN
        RETURN jsonb_build_object(
            'protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object(
                'code','STALE_AGENT_RUN_OWNER','message','Agent Run owner is stale','status',409
            )
        );
    END IF;
    IF EXISTS (
        SELECT 1 FROM __forest_module_schema__.run_cancel_requests c
        WHERE c.bot_id=requested_bot AND c.run_id=requested_run
    ) THEN
        RETURN jsonb_build_object(
            'protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object(
                'code','AGENT_CANCEL_REQUESTED',
                'message','Agent Run cancellation won the external handoff race','status',409
            )
        );
    END IF;
    SELECT * INTO request_row
    FROM __forest_module_schema__.model_requests
    WHERE bot_id=requested_bot AND request_id=requested_request FOR UPDATE;
    SELECT * INTO attempt_row
    FROM __forest_module_schema__.model_transport_attempts
    WHERE bot_id=requested_bot AND attempt_id=requested_attempt FOR UPDATE;
      IF request_row.request_id IS NULL OR attempt_row.attempt_id IS NULL
         OR request_row.run_id IS DISTINCT FROM requested_run THEN
        RETURN jsonb_build_object(
            'protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object(
                'code','MODEL_ATTEMPT_NOT_FOUND','message','Model attempt does not exist','status',404
            )
        );
      END IF;
      IF effect_handoff->>'effect_operation_id'
            IS DISTINCT FROM attempt_row.effect_operation_ref
         OR effect_handoff->>'wire_payload_id'
            IS DISTINCT FROM attempt_row.wire_payload_id
         OR effect_handoff->>'wire_payload_digest'
            IS DISTINCT FROM attempt_row.wire_payload_digest
         OR effect_handoff->>'executor_component_id'
            IS DISTINCT FROM request_row.provider_component_id
         OR effect_handoff->>'executor_generation_id'
            IS DISTINCT FROM request_row.provider_generation_id THEN
          RETURN jsonb_build_object(
              'protocol','forest.domain-result/1','ok',false,
              'error',jsonb_build_object(
                  'code','AGENT_MODEL_HANDOFF_INPUT_INVALID',
                  'message','Effect handoff differs from the frozen model attempt','status',409
              )
          );
      END IF;
    IF request_row.current_attempt_id IS DISTINCT FROM requested_attempt
       OR attempt_row.request_id IS DISTINCT FROM requested_request THEN
        RETURN jsonb_build_object(
            'protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object(
                'code','MODEL_ATTEMPT_CONFLICT','message','Current model attempt differs','status',409
            )
        );
    END IF;
    IF request_row.state='DISPATCHING' AND attempt_row.state='DISPATCHING' THEN
        RETURN jsonb_build_object(
            'protocol','forest.domain-result/1','ok',true,
            'value',jsonb_build_object(
                'attempt_id',attempt_row.attempt_id,'request_id',attempt_row.request_id,
                'attempt_no',attempt_row.attempt_no,
                'effect_operation_ref',attempt_row.effect_operation_ref,
                'state',attempt_row.state
            )
        );
    END IF;
    IF run_row.state IS DISTINCT FROM 'PREPARING_MODEL'
       OR request_row.state IS DISTINCT FROM 'ATTEMPT_PREPARED'
       OR attempt_row.state IS DISTINCT FROM 'PREPARED' THEN
        RETURN jsonb_build_object(
            'protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object(
                'code','MODEL_ATTEMPT_CONFLICT','message','Model handoff boundary closed','status',409
            )
        );
    END IF;
    UPDATE __forest_module_schema__.model_transport_attempts
    SET state='DISPATCHING',started_at=run_row.database_now
    WHERE bot_id=requested_bot AND attempt_id=requested_attempt;
    UPDATE __forest_module_schema__.model_requests
    SET state='DISPATCHING',updated_at=clock_timestamp()
    WHERE bot_id=requested_bot AND request_id=requested_request;
    UPDATE __forest_module_schema__.agent_runs
    SET state='MODEL_PENDING',current_model_request_id=requested_request,
        updated_at=clock_timestamp()
    WHERE bot_id=requested_bot AND run_id=requested_run;
    RETURN jsonb_build_object(
        'protocol','forest.domain-result/1','ok',true,
        'value',jsonb_build_object(
            'attempt_id',attempt_row.attempt_id,'request_id',attempt_row.request_id,
            'attempt_no',attempt_row.attempt_no,
            'effect_operation_ref',attempt_row.effect_operation_ref,'state','DISPATCHING'
        )
    );
END;
$$;

CREATE FUNCTION agent_observe_model_result(context jsonb, envelope jsonb)
RETURNS jsonb
LANGUAGE plpgsql
VOLATILE
SET search_path = pg_catalog
AS $$
DECLARE
    body jsonb := envelope->'input';
    owner_value jsonb := body->'owner';
    requested_bot text := context->>'bot_id';
    requested_run text := owner_value->>'run_id';
    requested_owner text := owner_value->>'owner_id';
    requested_epoch bigint := (owner_value->>'epoch')::bigint;
    requested_operation text := body->>'observation_operation_id';
    requested_request text := body->>'request_id';
    requested_attempt text := body->>'attempt_id';
    observed_state_value text := body->>'observed_state';
    evidence_kind_value text := body->>'evidence_kind';
    effect_observation_value text := body->>'effect_observation_ref';
    result_value jsonb := NULL;
    usage_value jsonb := NULL;
    input_digest_value text;
    run_row record;
    request_row record;
    attempt_row record;
    replay record;
    previous_value text;
    resulting_value text;
    accepted_value boolean := false;
    conflict_value text := NULL;
    settled_value timestamptz;
    resolved_value timestamptz := NULL;
      run_state_value text;
      effect_preparation jsonb := envelope->'prior'->(body->>'effect_step_id');
      effect_observation jsonb := effect_preparation->'observation';
      effect_evidence jsonb := effect_preparation->'evidence_payload';
      effect_evidence_parents jsonb := effect_preparation->'evidence_parents';
BEGIN
    SELECT item->'payload' INTO result_value
    FROM jsonb_array_elements(body->'payloads') item
    WHERE item->>'role'='RESULT' LIMIT 1;
    SELECT item->'payload' INTO usage_value
    FROM jsonb_array_elements(body->'payloads') item
    WHERE item->>'role'='USAGE' LIMIT 1;
    IF envelope->>'protocol' IS DISTINCT FROM 'forest.uow-step/1'
         OR envelope#>>'{caller,component_id}' IS DISTINCT FROM 'agent.runtime'
         OR body->>'effect_step_id' IS DISTINCT FROM 'effect'
         OR effect_preparation->>'protocol'
            IS DISTINCT FROM 'forest.effect-command-observation/1'
         OR effect_evidence->>'owner_component_id' IS DISTINCT FROM 'kernel.effect'
         OR effect_evidence->>'owner_kind' IS DISTINCT FROM 'effect.evidence'
         OR effect_evidence->>'owner_id' IS DISTINCT FROM requested_operation
         OR effect_evidence->>'digest' IS DISTINCT FROM body->>'evidence_digest'
         OR effect_observation->>'attempt_id' IS DISTINCT FROM requested_attempt
         OR effect_observation->>'observation_operation_id'
            IS DISTINCT FROM requested_operation
         OR effect_observation->>'observed_status'
            IS DISTINCT FROM observed_state_value
         OR lower(effect_observation->>'evidence_kind')
            IS DISTINCT FROM lower(evidence_kind_value)
         OR (effect_observation->>'accepted')::boolean IS DISTINCT FROM true
         OR effect_observation->>'resulting_state'
            IS DISTINCT FROM observed_state_value
         OR jsonb_typeof(effect_evidence_parents) IS DISTINCT FROM 'array'
         OR jsonb_array_length(effect_evidence_parents)
            IS DISTINCT FROM jsonb_array_length(body->'payloads')
         OR EXISTS (
             SELECT 1 FROM jsonb_array_elements(body->'payloads') item
             WHERE NOT EXISTS (
                 SELECT 1 FROM jsonb_array_elements(effect_evidence_parents) parent
                 WHERE parent = item->'payload'
             )
         )
         OR EXISTS (
             SELECT 1 FROM jsonb_array_elements(effect_evidence_parents) parent
             WHERE NOT EXISTS (
                 SELECT 1 FROM jsonb_array_elements(body->'payloads') item
                 WHERE item->'payload' = parent
             )
         )
         OR (SELECT count(*)<>count(DISTINCT item->>'role')
           FROM jsonb_array_elements(body->'payloads') item)
       OR (observed_state_value='SUCCEEDED' AND result_value IS NULL)
       OR (result_value IS NOT NULL AND result_value->>'owner_kind' IS DISTINCT FROM 'model.result')
       OR (usage_value IS NOT NULL AND usage_value->>'owner_kind' IS DISTINCT FROM 'model.usage')
       OR (result_value IS NOT NULL AND result_value->>'owner_id' IS DISTINCT FROM requested_attempt)
       OR (usage_value IS NOT NULL AND usage_value->>'owner_id' IS DISTINCT FROM requested_attempt) THEN
        RETURN jsonb_build_object(
            'protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object(
                'code','AGENT_MODEL_OBSERVATION_INPUT_INVALID',
                'message','Model observation envelope is invalid','status',422
            )
        );
    END IF;
    input_digest_value := 'sha256:' || encode(sha256(convert_to(jsonb_build_object(
        'request_id',requested_request,'attempt_id',requested_attempt,
        'observed_state',observed_state_value,'evidence_kind',evidence_kind_value,
        'effect_observation_ref',effect_observation_value,
        'result',result_value,'usage',usage_value
    )::text,'UTF8')),'hex');
    SELECT *,clock_timestamp() AS database_now INTO run_row
    FROM __forest_module_schema__.agent_runs
    WHERE bot_id=requested_bot AND run_id=requested_run FOR UPDATE;
    IF NOT FOUND OR run_row.owner_id IS DISTINCT FROM requested_owner
       OR run_row.owner_epoch IS DISTINCT FROM requested_epoch
       OR run_row.owner_expires_at IS NULL
       OR run_row.owner_expires_at<=run_row.database_now THEN
        RETURN jsonb_build_object(
            'protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object(
                'code','STALE_AGENT_RUN_OWNER','message','Agent Run owner is stale','status',409
            )
        );
    END IF;
    PERFORM pg_advisory_xact_lock(hashtextextended(
        'forest.agent.runtime/model-observation/' || requested_bot || '/' || requested_operation,0
    ));
    SELECT * INTO replay
    FROM __forest_module_schema__.model_observations
    WHERE bot_id=requested_bot AND observation_operation_id=requested_operation;
    IF FOUND THEN
        IF replay.input_digest IS DISTINCT FROM input_digest_value THEN
            RETURN jsonb_build_object(
                'protocol','forest.domain-result/1','ok',false,
                'error',jsonb_build_object(
                    'code','MODEL_OBSERVATION_CONFLICT',
                    'message','Model observation input differs','status',409
                )
            );
        END IF;
        RETURN jsonb_build_object(
            'protocol','forest.domain-result/1','ok',true,
            'value',jsonb_build_object(
                'observation_operation_id',replay.observation_operation_id,
                'attempt_id',replay.attempt_id,'evidence_kind',replay.evidence_kind,
                'observed_state',replay.observed_state,'accepted',replay.accepted,
                'previous_state',replay.previous_state,
                'resulting_state',replay.resulting_state,
                'conflict_code',replay.conflict_code
            )
        );
    END IF;
    SELECT * INTO request_row
    FROM __forest_module_schema__.model_requests
    WHERE bot_id=requested_bot AND request_id=requested_request FOR UPDATE;
    SELECT * INTO attempt_row
    FROM __forest_module_schema__.model_transport_attempts
    WHERE bot_id=requested_bot AND attempt_id=requested_attempt FOR UPDATE;
    IF request_row.request_id IS NULL OR attempt_row.attempt_id IS NULL
       OR request_row.run_id IS DISTINCT FROM requested_run
       OR request_row.current_attempt_id IS DISTINCT FROM requested_attempt
       OR attempt_row.request_id IS DISTINCT FROM requested_request THEN
        RETURN jsonb_build_object(
            'protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object(
                'code','MODEL_ATTEMPT_NOT_FOUND','message','Model attempt does not exist','status',404
            )
        );
    END IF;
    IF (result_value IS NOT NULL AND result_value->>'owner_component_id'
            IS DISTINCT FROM request_row.provider_component_id)
       OR (usage_value IS NOT NULL AND usage_value->>'owner_component_id'
            IS DISTINCT FROM request_row.provider_component_id) THEN
        RETURN jsonb_build_object(
            'protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object(
                'code','MODEL_RESULT_IDENTITY_CONFLICT',
                'message','Model result owner differs from the frozen provider','status',409
            )
        );
    END IF;
    previous_value := attempt_row.state;
    IF previous_value NOT IN ('DISPATCHING','SUCCEEDED','NOT_APPLIED','UNKNOWN') THEN
        RETURN jsonb_build_object(
            'protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object(
                'code','MODEL_OBSERVATION_BOUNDARY_CLOSED',
                'message','Model attempt has not crossed dispatch','status',409
            )
        );
    END IF;
    IF previous_value='DISPATCHING' THEN
        accepted_value := true;
    ELSIF previous_value='UNKNOWN' AND evidence_kind_value='RECONCILIATION'
          AND observed_state_value IN ('SUCCEEDED','NOT_APPLIED') THEN
        accepted_value := true;
    ELSE
        conflict_value := 'MODEL_RESULT_ALREADY_SETTLED';
    END IF;
    resulting_value := CASE WHEN accepted_value THEN observed_state_value ELSE previous_value END;
    INSERT INTO __forest_module_schema__.model_observations(
        bot_id,observation_operation_id,attempt_id,input_digest,evidence_kind,
        observed_state,effect_observation_ref,result_payload_id,result_payload_digest,
        usage_payload_id,usage_payload_digest,accepted,previous_state,resulting_state,
        conflict_code
    ) VALUES (
        requested_bot,requested_operation,requested_attempt,input_digest_value,
        evidence_kind_value,observed_state_value,effect_observation_value,
        result_value->>'payload_id',result_value->>'digest',
        usage_value->>'payload_id',usage_value->>'digest',accepted_value,
        previous_value,resulting_value,conflict_value
    );
    IF accepted_value THEN
        settled_value := run_row.database_now;
        IF previous_value='UNKNOWN' THEN
            resolved_value := settled_value;
        END IF;
        UPDATE __forest_module_schema__.model_transport_attempts
        SET state=observed_state_value,
            result_payload_id=result_value->>'payload_id',
            result_payload_digest=result_value->>'digest',
            usage_payload_id=usage_value->>'payload_id',
            usage_payload_digest=usage_value->>'digest',
            settled_at=COALESCE(settled_at,settled_value),resolved_at=resolved_value
        WHERE bot_id=requested_bot AND attempt_id=requested_attempt;
        UPDATE __forest_module_schema__.model_requests
        SET state=observed_state_value,updated_at=clock_timestamp()
        WHERE bot_id=requested_bot AND request_id=requested_request;
        run_state_value := CASE observed_state_value
            WHEN 'SUCCEEDED' THEN 'PARSING'
            WHEN 'NOT_APPLIED' THEN 'PREPARING_MODEL'
            ELSE 'BLOCKED_UNKNOWN'
        END;
        UPDATE __forest_module_schema__.agent_runs
        SET state=run_state_value,updated_at=clock_timestamp()
        WHERE bot_id=requested_bot AND run_id=requested_run;
    END IF;
    RETURN jsonb_build_object(
        'protocol','forest.domain-result/1','ok',true,
        'value',jsonb_build_object(
            'observation_operation_id',requested_operation,
            'attempt_id',requested_attempt,'evidence_kind',evidence_kind_value,
            'observed_state',observed_state_value,'accepted',accepted_value,
            'previous_state',previous_value,'resulting_state',resulting_value,
            'conflict_code',conflict_value
        )
    );
END;
$$;

CREATE FUNCTION agent_record_protocol_failure(context jsonb, envelope jsonb)
RETURNS jsonb
LANGUAGE plpgsql
VOLATILE
SET search_path = pg_catalog
AS $$
DECLARE
    body jsonb := envelope->'input';
    owner_value jsonb := body->'owner';
    feedback_value jsonb := body->'feedback';
    checkpoint_value jsonb := body->'checkpoint_state';
    requested_bot text := context->>'bot_id';
    requested_run text := owner_value->>'run_id';
    requested_owner text := owner_value->>'owner_id';
    requested_epoch bigint := (owner_value->>'epoch')::bigint;
    requested_failure text := body->>'failure_id';
    requested_request text := body->>'request_id';
    requested_attempt text := body->>'attempt_id';
    requested_error text := body->>'error_code';
    requested_candidate_digest text := body->>'candidate_digest';
    requested_checkpoint text := body->>'checkpoint_id';
    allow_repair_value boolean := (body->>'allow_repair')::boolean;
    target_phase text := CASE WHEN allow_repair_value THEN 'REPAIR_READY' ELSE 'FAILED' END;
    input_digest_value text;
    checkpoint_digest_value text;
    checksum_value text;
    run_row record;
    request_row record;
    attempt_row record;
    existing record;
    next_step_value integer;
    repair_step_value integer := NULL;
    sequence_value bigint;
BEGIN
    IF envelope->>'protocol' IS DISTINCT FROM 'forest.uow-step/1'
       OR envelope#>>'{caller,component_id}' IS DISTINCT FROM 'agent.runtime'
       OR feedback_value->>'owner_component_id' IS DISTINCT FROM 'agent.runtime'
       OR feedback_value->>'owner_kind' IS DISTINCT FROM 'protocol.repair.feedback'
       OR feedback_value->>'owner_id' IS DISTINCT FROM requested_failure
       OR checkpoint_value->>'owner_component_id' IS DISTINCT FROM 'agent.runtime'
       OR checkpoint_value->>'owner_kind' IS DISTINCT FROM 'checkpoint.state'
       OR checkpoint_value->>'owner_id' IS DISTINCT FROM requested_checkpoint THEN
        RETURN jsonb_build_object(
            'protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object(
                'code','AGENT_PROTOCOL_FAILURE_INPUT_INVALID',
                'message','Protocol failure envelope is invalid','status',422
            )
        );
    END IF;
    input_digest_value := 'sha256:' || encode(sha256(convert_to(jsonb_build_object(
        'run_id',requested_run,'owner_id',requested_owner,'owner_epoch',requested_epoch,
        'failure_id',requested_failure,'request_id',requested_request,
        'attempt_id',requested_attempt,'error_code',requested_error,
        'candidate_digest',requested_candidate_digest,'feedback',feedback_value,
        'checkpoint_id',requested_checkpoint,'checkpoint_state',checkpoint_value,
        'allow_repair',allow_repair_value
    )::text,'UTF8')),'hex');
    SELECT *,clock_timestamp() AS database_now INTO run_row
    FROM __forest_module_schema__.agent_runs
    WHERE bot_id=requested_bot AND run_id=requested_run FOR UPDATE;
    IF NOT FOUND OR run_row.owner_id IS DISTINCT FROM requested_owner
       OR run_row.owner_epoch IS DISTINCT FROM requested_epoch
       OR run_row.owner_expires_at IS NULL
       OR run_row.owner_expires_at<=run_row.database_now THEN
        RETURN jsonb_build_object(
            'protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object(
                'code','STALE_AGENT_RUN_OWNER','message','Agent Run owner is stale','status',409
            )
        );
    END IF;
    PERFORM pg_advisory_xact_lock(hashtextextended(
        'forest.agent.runtime/protocol-failure/' || requested_bot || '/' || requested_request,0
    ));
    SELECT * INTO existing
    FROM __forest_module_schema__.protocol_failures
    WHERE bot_id=requested_bot
      AND (failure_id=requested_failure OR failed_request_id=requested_request)
    ORDER BY CASE WHEN failure_id=requested_failure THEN 0 ELSE 1 END
    LIMIT 1;
    IF FOUND THEN
        IF existing.failure_id IS DISTINCT FROM requested_failure
           OR existing.input_digest IS DISTINCT FROM input_digest_value THEN
            RETURN jsonb_build_object(
                'protocol','forest.domain-result/1','ok',false,
                'error',jsonb_build_object(
                    'code','AGENT_PROTOCOL_FAILURE_CONFLICT',
                    'message','Protocol failure identity differs','status',409
                )
            );
        END IF;
        RETURN jsonb_build_object(
            'protocol','forest.domain-result/1','ok',true,
            'value',jsonb_build_object(
                'failure_id',existing.failure_id,
                'failed_request_id',existing.failed_request_id,
                'error_code',existing.error_code,
                'candidate_digest',existing.candidate_digest,
                'resulting_state',existing.resulting_state,
                'repair_step_seq',existing.repair_step_seq
            )
        );
    END IF;
    SELECT * INTO request_row
    FROM __forest_module_schema__.model_requests
    WHERE bot_id=requested_bot AND request_id=requested_request FOR UPDATE;
    SELECT * INTO attempt_row
    FROM __forest_module_schema__.model_transport_attempts
    WHERE bot_id=requested_bot AND attempt_id=requested_attempt FOR SHARE;
    IF request_row.request_id IS NULL OR attempt_row.attempt_id IS NULL
       OR run_row.state IS DISTINCT FROM 'PARSING'
       OR run_row.current_model_request_id IS DISTINCT FROM requested_request
       OR request_row.run_id IS DISTINCT FROM requested_run
       OR request_row.step_seq IS DISTINCT FROM run_row.next_step_seq
       OR request_row.state IS DISTINCT FROM 'SUCCEEDED'
       OR request_row.current_attempt_id IS DISTINCT FROM requested_attempt
       OR attempt_row.request_id IS DISTINCT FROM requested_request
       OR attempt_row.state IS DISTINCT FROM 'SUCCEEDED'
       OR attempt_row.result_payload_id IS NULL THEN
        RETURN jsonb_build_object(
            'protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object(
                'code','AGENT_PROTOCOL_FAILURE_BOUNDARY_CLOSED',
                'message','Model response is not the current parse boundary','status',409
            )
        );
    END IF;
    IF allow_repair_value AND run_row.repair_count>=1 THEN
        RETURN jsonb_build_object(
            'protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object(
                'code','AGENT_PROTOCOL_REPAIR_EXHAUSTED',
                'message','The bounded protocol repair was already consumed','status',409
            )
        );
    END IF;
    next_step_value := run_row.next_step_seq+1;
    IF allow_repair_value THEN
        repair_step_value := next_step_value;
    END IF;
    sequence_value := run_row.checkpoint_seq+1;
    checkpoint_digest_value := 'sha256:' || encode(sha256(convert_to(jsonb_build_object(
        'run_id',requested_run,'failure_id',requested_failure,'phase',target_phase,
        'input_revision',run_row.input_revision,'next_step_seq',next_step_value,
        'state',checkpoint_value,'owner_epoch',requested_epoch
    )::text,'UTF8')),'hex');
    checksum_value := 'sha256:' || encode(sha256(convert_to(jsonb_build_array(
        requested_run,sequence_value,target_phase,run_row.input_revision,
        next_step_value,checkpoint_value->>'digest',requested_epoch
    )::text,'UTF8')),'hex');
    INSERT INTO __forest_module_schema__.run_checkpoints(
        bot_id,run_id,checkpoint_seq,checkpoint_id,input_digest,phase,input_revision,
        next_step_seq,state_payload_id,state_payload_digest,state_owner_component_id,
        state_owner_kind,state_owner_id,checkpoint_checksum,owner_epoch
    ) VALUES (
        requested_bot,requested_run,sequence_value,requested_checkpoint,
        checkpoint_digest_value,target_phase,run_row.input_revision,next_step_value,
        checkpoint_value->>'payload_id',checkpoint_value->>'digest',
        checkpoint_value->>'owner_component_id',checkpoint_value->>'owner_kind',
        checkpoint_value->>'owner_id',checksum_value,requested_epoch
    );
    INSERT INTO __forest_module_schema__.protocol_failures(
        bot_id,failure_id,input_digest,run_id,failed_request_id,failed_attempt_id,
        error_code,candidate_digest,feedback_payload_id,feedback_payload_digest,
        feedback_owner_component_id,feedback_owner_kind,feedback_owner_id,
        resulting_state,repair_step_seq
    ) VALUES (
        requested_bot,requested_failure,input_digest_value,requested_run,requested_request,
        requested_attempt,requested_error,requested_candidate_digest,
        feedback_value->>'payload_id',feedback_value->>'digest',
        feedback_value->>'owner_component_id',feedback_value->>'owner_kind',
        feedback_value->>'owner_id',target_phase,repair_step_value
    );
    UPDATE __forest_module_schema__.agent_runs
    SET state=target_phase,next_step_seq=next_step_value,checkpoint_seq=sequence_value,
        repair_count=repair_count+CASE WHEN allow_repair_value THEN 1 ELSE 0 END,
        current_model_request_id=NULL,
        error_code=CASE WHEN target_phase='FAILED' THEN 'AGENT_PROTOCOL_FAILED' ELSE NULL END,
        terminal_at=CASE WHEN target_phase='FAILED' THEN run_row.database_now ELSE NULL END,
        updated_at=clock_timestamp()
    WHERE bot_id=requested_bot AND run_id=requested_run;
    IF target_phase='FAILED' THEN
        UPDATE __forest_module_schema__.agent_sessions
        SET state='FAILED',active_run_id=NULL,
            terminal_reason_code='AGENT_PROTOCOL_FAILED',closed_at=clock_timestamp(),
            updated_at=clock_timestamp()
        WHERE bot_id=requested_bot AND session_id=run_row.session_id
          AND state='ACTIVE' AND active_run_id=requested_run;
    END IF;
    RETURN jsonb_build_object(
        'protocol','forest.domain-result/1','ok',true,
        'value',jsonb_build_object(
            'failure_id',requested_failure,'failed_request_id',requested_request,
            'error_code',requested_error,'candidate_digest',requested_candidate_digest,
            'resulting_state',target_phase,'repair_step_seq',repair_step_value
        )
    );
END;
$$;

CREATE FUNCTION agent_record_model_turn(context jsonb, envelope jsonb)
RETURNS jsonb
LANGUAGE plpgsql
VOLATILE
SET search_path = pg_catalog
AS $$
DECLARE
    body jsonb := envelope->'input';
    authority_value jsonb := envelope#>'{prior,authority}';
    owner_value jsonb := body->'owner';
    turn_value jsonb := body->'turn';
    blocks_value jsonb := turn_value->'blocks';
    tool_value jsonb := NULLIF(body->'tool','null'::jsonb);
    turn_artifact jsonb := NULL;
    assistant_artifact jsonb := NULL;
    arguments_artifact jsonb := NULL;
    function_value jsonb := NULL;
    requested_bot text := context->>'bot_id';
    requested_run text := owner_value->>'run_id';
    requested_owner text := owner_value->>'owner_id';
    requested_epoch bigint := (owner_value->>'epoch')::bigint;
    requested_turn text := body->>'turn_id';
    requested_request text := body->>'request_id';
    repair_request_value text := body->>'repair_of_request_id';
    assistant_count integer;
    function_count integer;
    thinking_count_value integer;
    max_thinking_position bigint;
    min_assistant_position bigint;
    function_position bigint;
    terminal_value boolean;
    thinking_only_value boolean;
    next_state_value text;
    run_row record;
    request_row record;
    existing record;
    stored_tool record;
    repair_row record;
    principal_value text := NULL;
BEGIN
    SELECT item->'payload' INTO turn_artifact
    FROM jsonb_array_elements(body->'artifacts') item WHERE item->>'role'='TURN' LIMIT 1;
    SELECT item->'payload' INTO assistant_artifact
    FROM jsonb_array_elements(body->'artifacts') item WHERE item->>'role'='ASSISTANT' LIMIT 1;
    SELECT item->'payload' INTO arguments_artifact
    FROM jsonb_array_elements(body->'artifacts') item
    WHERE item->>'role'='TOOL_ARGUMENTS' LIMIT 1;
    SELECT count(*) FILTER (WHERE item->>'block_type'='Assistant'),
           count(*) FILTER (WHERE item->>'block_type'='FunctionCall'),
           count(*) FILTER (WHERE item->>'block_type'='Thinking'),
           max(position) FILTER (WHERE item->>'block_type'='Thinking'),
           min(position) FILTER (WHERE item->>'block_type'='Assistant'),
           max(position) FILTER (WHERE item->>'block_type'='FunctionCall')
    INTO assistant_count,function_count,thinking_count_value,
         max_thinking_position,min_assistant_position,function_position
    FROM jsonb_array_elements(blocks_value) WITH ORDINALITY AS block(item,position);
    SELECT item INTO function_value
    FROM jsonb_array_elements(blocks_value) item
    WHERE item->>'block_type'='FunctionCall' LIMIT 1;
    terminal_value := function_count=0 AND assistant_count>0;
    thinking_only_value := thinking_count_value=jsonb_array_length(blocks_value);
    next_state_value := CASE
        WHEN function_count=1 THEN 'WAIT_TOOL'
        WHEN terminal_value THEN 'SAFE_BOUNDARY'
        ELSE 'PREPARING_MODEL'
    END;
    IF envelope->>'protocol' IS DISTINCT FROM 'forest.uow-step/1'
       OR envelope#>>'{caller,component_id}' IS DISTINCT FROM 'agent.runtime'
       OR (SELECT count(*)<>count(DISTINCT item->>'role')
           FROM jsonb_array_elements(body->'artifacts') item)
       OR turn_artifact IS NULL
       OR turn_artifact->>'owner_kind' IS DISTINCT FROM 'model.turn'
       OR turn_artifact->>'owner_id' IS DISTINCT FROM requested_turn
       OR function_count>1
       OR (function_count=1 AND function_position<>jsonb_array_length(blocks_value))
       OR (min_assistant_position IS NOT NULL AND max_thinking_position IS NOT NULL
           AND max_thinking_position>min_assistant_position)
       OR ((assistant_count>0) IS DISTINCT FROM (assistant_artifact IS NOT NULL))
       OR (assistant_artifact IS NOT NULL
           AND (assistant_artifact->>'owner_kind' IS DISTINCT FROM 'assistant.candidate'
                OR assistant_artifact->>'owner_id' IS DISTINCT FROM requested_turn))
       OR ((function_count=1) IS DISTINCT FROM (tool_value IS NOT NULL))
       OR ((function_count=1) IS DISTINCT FROM (arguments_artifact IS NOT NULL))
       OR (arguments_artifact IS NOT NULL
           AND (arguments_artifact->>'owner_kind' IS DISTINCT FROM 'tool.arguments'
                OR arguments_artifact->>'owner_id'
                   IS DISTINCT FROM tool_value->>'tool_call_id'))
       OR ((turn_value->>'truncated_after_function_call')::boolean AND function_count<>1)
       OR (function_count=1 AND (
            function_value->>'name' IS NULL
            OR function_value#>>'{arguments,authority_trigger_id}'
                IS DISTINCT FROM tool_value->>'authority_trigger_ref')) THEN
        RETURN jsonb_build_object(
            'protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object(
                'code','AGENT_MODEL_TURN_INPUT_INVALID',
                'message','ModelTurn envelope is invalid','status',422
            )
        );
    END IF;
    SELECT *,clock_timestamp() AS database_now INTO run_row
    FROM __forest_module_schema__.agent_runs
    WHERE bot_id=requested_bot AND run_id=requested_run FOR UPDATE;
    IF NOT FOUND OR run_row.owner_id IS DISTINCT FROM requested_owner
       OR run_row.owner_epoch IS DISTINCT FROM requested_epoch
       OR run_row.owner_expires_at IS NULL
       OR run_row.owner_expires_at<=run_row.database_now THEN
        RETURN jsonb_build_object(
            'protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object(
                'code','STALE_AGENT_RUN_OWNER','message','Agent Run owner is stale','status',409
            )
        );
    END IF;
    IF authority_value->>'protocol' IS DISTINCT FROM
            'forest.conversation-run-trigger-authority/1'
       OR authority_value->>'run_ref' IS DISTINCT FROM requested_run
       OR authority_value->>'trigger_ref' IS DISTINCT FROM
            tool_value->>'authority_trigger_ref'
       OR (tool_value IS NULL AND (
            authority_value->>'session_ref' IS NOT NULL
            OR authority_value->>'conversation_ref' IS NOT NULL
            OR authority_value->>'principal_ref' IS NOT NULL
       ))
       OR (tool_value IS NOT NULL AND (
            authority_value->>'principal_ref' IS NULL
            OR NOT EXISTS (
                SELECT 1 FROM __forest_module_schema__.agent_sessions s
                WHERE s.bot_id=requested_bot AND s.session_id=run_row.session_id
                  AND s.session_id=authority_value->>'session_ref'
                  AND s.conversation_ref=authority_value->>'conversation_ref'
            )
       )) THEN
        RETURN jsonb_build_object(
            'protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object(
                'code','AGENT_TOOL_AUTHORITY_INVALID',
                'message','Conversation trigger authority differs from this Run','status',403
            )
        );
    END IF;
    PERFORM pg_advisory_xact_lock(hashtextextended(
        'forest.agent.runtime/model-turn/' || requested_bot || '/' || requested_request,0
    ));
    SELECT * INTO existing
    FROM __forest_module_schema__.model_turns
    WHERE bot_id=requested_bot AND request_id=requested_request;
    IF FOUND THEN
        SELECT * INTO stored_tool
        FROM __forest_module_schema__.tool_calls
        WHERE bot_id=requested_bot AND turn_id=existing.turn_id;
        IF existing.turn_id IS DISTINCT FROM requested_turn
           OR existing.turn_payload_id IS DISTINCT FROM turn_artifact->>'payload_id'
           OR existing.turn_payload_digest IS DISTINCT FROM turn_artifact->>'digest'
           OR existing.assistant_payload_id
                IS DISTINCT FROM assistant_artifact->>'payload_id'
           OR existing.assistant_payload_digest
                IS DISTINCT FROM assistant_artifact->>'digest'
           OR existing.function_call_payload_id
                IS DISTINCT FROM arguments_artifact->>'payload_id'
           OR existing.function_call_payload_digest
                IS DISTINCT FROM arguments_artifact->>'digest'
           OR existing.terminal IS DISTINCT FROM terminal_value
           OR existing.thinking_only IS DISTINCT FROM thinking_only_value
           OR existing.truncated_after_function_call
                IS DISTINCT FROM (turn_value->>'truncated_after_function_call')::boolean
           OR existing.repair_of_request_id IS DISTINCT FROM repair_request_value
           OR ((stored_tool.tool_call_id IS NOT NULL) IS DISTINCT FROM (tool_value IS NOT NULL))
           OR (tool_value IS NOT NULL AND (
                stored_tool.tool_call_id IS DISTINCT FROM tool_value->>'tool_call_id'
                OR stored_tool.tool_name IS DISTINCT FROM function_value->>'name'
                OR stored_tool.arguments_payload_id
                    IS DISTINCT FROM arguments_artifact->>'payload_id'
                OR stored_tool.arguments_payload_digest
                    IS DISTINCT FROM arguments_artifact->>'digest'
                OR stored_tool.authority_trigger_ref
                    IS DISTINCT FROM tool_value->>'authority_trigger_ref'
                OR stored_tool.tool_component_id IS DISTINCT FROM tool_value->>'component_id'
                OR stored_tool.tool_generation_id IS DISTINCT FROM tool_value->>'generation_id'
                OR stored_tool.tool_service_id IS DISTINCT FROM tool_value->>'service_id'
                OR stored_tool.tool_contract_digest
                    IS DISTINCT FROM tool_value->>'tool_contract_digest'
                OR stored_tool.tool_binding_digest
                    IS DISTINCT FROM tool_value->>'tool_binding_digest'
                OR stored_tool.approval_required
                    IS DISTINCT FROM (tool_value->>'approval_required')::boolean
                OR stored_tool.supports_query
                    IS DISTINCT FROM (tool_value->>'supports_query')::boolean
                OR stored_tool.effect_operation_ref
                    IS DISTINCT FROM tool_value->>'effect_operation_ref')) THEN
            RETURN jsonb_build_object(
                'protocol','forest.domain-result/1','ok',false,
                'error',jsonb_build_object(
                    'code','MODEL_TURN_ID_CONFLICT','message','ModelTurn identity differs','status',409
                )
            );
        END IF;
        RETURN jsonb_build_object(
            'protocol','forest.domain-result/1','ok',true,
            'value',jsonb_build_object(
                'turn_id',existing.turn_id,'request_id',existing.request_id,
                'step_seq',existing.step_seq,'terminal',existing.terminal,
                'thinking_only',existing.thinking_only,
                'truncated_after_function_call',existing.truncated_after_function_call,
                'next_state',CASE WHEN stored_tool.tool_call_id IS NOT NULL THEN 'WAIT_TOOL'
                    WHEN existing.terminal THEN 'SAFE_BOUNDARY' ELSE 'PREPARING_MODEL' END,
                'tool_call_id',stored_tool.tool_call_id
            )
        );
    END IF;
    SELECT * INTO request_row
    FROM __forest_module_schema__.model_requests
    WHERE bot_id=requested_bot AND request_id=requested_request FOR SHARE;
    IF request_row.request_id IS NULL OR request_row.run_id IS DISTINCT FROM requested_run
       OR request_row.state IS DISTINCT FROM 'SUCCEEDED'
       OR request_row.step_seq IS DISTINCT FROM run_row.next_step_seq
       OR run_row.state IS DISTINCT FROM 'PARSING' THEN
        RETURN jsonb_build_object(
            'protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object(
                'code','MODEL_TURN_BOUNDARY_CLOSED','message','ModelTurn boundary closed','status',409
            )
        );
    END IF;
    IF request_row.request_kind='REPAIR' THEN
        SELECT * INTO repair_row
        FROM __forest_module_schema__.protocol_failures
        WHERE bot_id=requested_bot AND run_id=requested_run
          AND repair_step_seq=request_row.step_seq;
        IF repair_row.failure_id IS NULL
           OR repair_request_value IS DISTINCT FROM repair_row.failed_request_id THEN
            RETURN jsonb_build_object(
                'protocol','forest.domain-result/1','ok',false,
                'error',jsonb_build_object(
                    'code','MODEL_TURN_REPAIR_CONFLICT',
                    'message','Repair ModelTurn does not name its failed request','status',409
                )
            );
        END IF;
    ELSIF repair_request_value IS NOT NULL THEN
        RETURN jsonb_build_object(
            'protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object(
                'code','MODEL_TURN_REPAIR_CONFLICT',
                'message','Non-repair ModelTurn names a failed request','status',409
            )
        );
    END IF;
    IF tool_value IS NOT NULL THEN
        principal_value := authority_value->>'principal_ref';
    END IF;
    INSERT INTO __forest_module_schema__.model_turns(
        bot_id,turn_id,run_id,request_id,step_seq,protocol_version,
        raw_candidate_digest,turn_payload_id,turn_payload_digest,
        turn_owner_component_id,turn_owner_kind,turn_owner_id,
        assistant_payload_id,assistant_payload_digest,function_call_payload_id,
        function_call_payload_digest,terminal,thinking_only,
        truncated_after_function_call,repair_of_request_id
    ) VALUES (
        requested_bot,requested_turn,requested_run,requested_request,request_row.step_seq,
        (turn_value->>'protocol_version')::integer,turn_value->>'raw_candidate_digest',
        turn_artifact->>'payload_id',turn_artifact->>'digest',
        turn_artifact->>'owner_component_id',turn_artifact->>'owner_kind',
        turn_artifact->>'owner_id',assistant_artifact->>'payload_id',
        assistant_artifact->>'digest',arguments_artifact->>'payload_id',
        arguments_artifact->>'digest',terminal_value,thinking_only_value,
        (turn_value->>'truncated_after_function_call')::boolean,repair_request_value
    );
    IF tool_value IS NOT NULL THEN
        INSERT INTO __forest_module_schema__.tool_calls(
            bot_id,tool_call_id,run_id,turn_id,step_seq,tool_name,
            tool_component_id,tool_generation_id,tool_service_id,
            arguments_payload_id,arguments_payload_digest,authority_trigger_ref,
            authority_principal_ref,tool_contract_digest,tool_binding_digest,
            approval_required,supports_query,effect_operation_ref
        ) VALUES (
            requested_bot,tool_value->>'tool_call_id',requested_run,requested_turn,
            request_row.step_seq,function_value->>'name',tool_value->>'component_id',
            tool_value->>'generation_id',tool_value->>'service_id',
            arguments_artifact->>'payload_id',
            arguments_artifact->>'digest',tool_value->>'authority_trigger_ref',
            principal_value,tool_value->>'tool_contract_digest',
            tool_value->>'tool_binding_digest',(tool_value->>'approval_required')::boolean,
            (tool_value->>'supports_query')::boolean,
            tool_value->>'effect_operation_ref'
        );
    END IF;
    UPDATE __forest_module_schema__.agent_runs
    SET state=next_state_value,next_step_seq=next_step_seq+1,
        current_model_request_id=NULL,
        current_tool_call_id=tool_value->>'tool_call_id',
        tool_count=tool_count+CASE WHEN tool_value IS NULL THEN 0 ELSE 1 END,
        thinking_count=thinking_count+CASE WHEN thinking_only_value THEN 1 ELSE 0 END,
        updated_at=clock_timestamp()
    WHERE bot_id=requested_bot AND run_id=requested_run;
    RETURN jsonb_build_object(
        'protocol','forest.domain-result/1','ok',true,
        'value',jsonb_build_object(
            'turn_id',requested_turn,'request_id',requested_request,
            'step_seq',request_row.step_seq,'terminal',terminal_value,
            'thinking_only',thinking_only_value,
            'truncated_after_function_call',
                (turn_value->>'truncated_after_function_call')::boolean,
            'next_state',next_state_value,'tool_call_id',tool_value->>'tool_call_id'
        )
    );
END;
$$;

CREATE FUNCTION agent_record_tool_approval_request(context jsonb, envelope jsonb)
RETURNS jsonb LANGUAGE plpgsql VOLATILE SET search_path = pg_catalog AS $$
DECLARE
    body jsonb := envelope->'input'; owner_value jsonb := body->'owner';
    approval_value jsonb := envelope->'prior'->(body->>'approval_step_id');
    requested_bot text := context->>'bot_id'; requested_run text := owner_value->>'run_id';
    requested_owner text := owner_value->>'owner_id'; requested_epoch bigint := (owner_value->>'epoch')::bigint;
    requested_tool text := body->>'tool_call_id'; requested_digest text := body->>'command_digest';
    requested_observation text := body->>'observation_operation_id';
    run_row record; tool_row record;
BEGIN
    IF envelope->>'protocol' IS DISTINCT FROM 'forest.uow-step/1'
       OR envelope#>>'{caller,component_id}' IS DISTINCT FROM 'agent.runtime'
       OR body->>'approval_step_id' IS DISTINCT FROM 'approval'
       OR approval_value->>'protocol' IS DISTINCT FROM 'forest.approval-state/1'
       OR approval_value->>'request_kind' IS DISTINCT FROM 'agent.tool'
       OR approval_value->>'request_id' IS DISTINCT FROM requested_tool
       OR approval_value->>'action' IS DISTINCT FROM 'tool.invoke'
       OR approval_value->>'input_digest' IS DISTINCT FROM requested_digest
       OR requested_observation IS NULL
       OR approval_value->>'state' IS DISTINCT FROM 'PENDING' THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,'error',jsonb_build_object(
            'code','AGENT_TOOL_APPROVAL_INPUT_INVALID','message','Tool approval request envelope is invalid','status',422));
    END IF;
    SELECT *,clock_timestamp() AS database_now INTO run_row FROM __forest_module_schema__.agent_runs
    WHERE bot_id=requested_bot AND run_id=requested_run FOR UPDATE;
    IF NOT FOUND OR run_row.owner_id IS DISTINCT FROM requested_owner
       OR run_row.owner_epoch IS DISTINCT FROM requested_epoch
       OR run_row.owner_expires_at IS NULL OR run_row.owner_expires_at<=run_row.database_now THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,'error',jsonb_build_object(
            'code','STALE_AGENT_RUN_OWNER','message','Agent Run owner is stale','status',409));
    END IF;
    SELECT * INTO tool_row FROM __forest_module_schema__.tool_calls
    WHERE bot_id=requested_bot AND tool_call_id=requested_tool FOR UPDATE;
    IF NOT FOUND OR tool_row.run_id IS DISTINCT FROM requested_run
       OR run_row.current_tool_call_id IS DISTINCT FROM requested_tool
       OR NOT tool_row.approval_required
       OR approval_value->>'plan_digest' IS DISTINCT FROM tool_row.tool_binding_digest THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,'error',jsonb_build_object(
            'code','AGENT_TOOL_APPROVAL_BINDING_CONFLICT','message','Approval differs from the frozen ToolCall','status',409));
    END IF;
    IF run_row.state='WAIT_APPROVAL' AND tool_row.state='WAIT_APPROVAL'
       AND tool_row.approval_ref=approval_value->>'approval_id' THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',true,'value',jsonb_build_object(
            'protocol','forest.agent-tool-approval/1','tool_call_id',requested_tool,
            'approval_ref',tool_row.approval_ref,'approval_state','PENDING','resulting_state','WAIT_APPROVAL'));
    END IF;
    IF run_row.state IS DISTINCT FROM 'WAIT_TOOL' OR tool_row.state IS DISTINCT FROM 'PROPOSED'
       OR tool_row.approval_ref IS NOT NULL THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,'error',jsonb_build_object(
            'code','AGENT_TOOL_APPROVAL_BOUNDARY_CLOSED','message','ToolCall is not at the approval boundary','status',409));
    END IF;
    INSERT INTO __forest_module_schema__.tool_observations(
        bot_id,observation_operation_id,tool_call_id,input_digest,observed_state,evidence_kind,
        approval_ref,accepted,previous_state,resulting_state)
    VALUES(requested_bot,requested_observation,requested_tool,requested_digest,'WAIT_APPROVAL',
        'AUTHORIZATION',approval_value->>'approval_id',true,'PROPOSED','WAIT_APPROVAL');
    UPDATE __forest_module_schema__.tool_calls SET state='WAIT_APPROVAL',
        approval_ref=approval_value->>'approval_id',updated_at=clock_timestamp()
    WHERE bot_id=requested_bot AND tool_call_id=requested_tool;
    UPDATE __forest_module_schema__.agent_runs SET state='WAIT_APPROVAL',updated_at=clock_timestamp()
    WHERE bot_id=requested_bot AND run_id=requested_run;
    RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',true,'value',jsonb_build_object(
        'protocol','forest.agent-tool-approval/1','tool_call_id',requested_tool,
        'approval_ref',approval_value->>'approval_id','approval_state','PENDING','resulting_state','WAIT_APPROVAL'));
END;
$$;

CREATE FUNCTION agent_resolve_tool_approval(context jsonb, envelope jsonb)
RETURNS jsonb LANGUAGE plpgsql VOLATILE SET search_path = pg_catalog AS $$
DECLARE
    body jsonb := envelope->'input'; owner_value jsonb := body->'owner';
    approval_value jsonb := envelope->'prior'->(body->>'approval_step_id');
    requested_bot text := context->>'bot_id'; requested_run text := owner_value->>'run_id';
    requested_owner text := owner_value->>'owner_id'; requested_epoch bigint := (owner_value->>'epoch')::bigint;
    requested_tool text := body->>'tool_call_id'; requested_digest text := body->>'command_digest';
    requested_observation text := body->>'observation_operation_id';
    approval_state text := approval_value->>'state'; resulting_state text;
    run_row record; tool_row record;
BEGIN
    IF envelope->>'protocol' IS DISTINCT FROM 'forest.uow-step/1'
       OR envelope#>>'{caller,component_id}' IS DISTINCT FROM 'agent.runtime'
       OR body->>'approval_step_id' IS DISTINCT FROM 'approval'
       OR approval_value->>'protocol' IS DISTINCT FROM 'forest.approval-state/1'
       OR approval_value->>'request_kind' IS DISTINCT FROM 'agent.tool'
       OR approval_value->>'request_id' IS DISTINCT FROM requested_tool
       OR approval_value->>'action' IS DISTINCT FROM 'tool.invoke'
       OR approval_value->>'input_digest' IS DISTINCT FROM requested_digest
       OR requested_observation IS NULL
       OR approval_state NOT IN ('PENDING','APPROVED','REJECTED','EXPIRED') THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,'error',jsonb_build_object(
            'code','AGENT_TOOL_APPROVAL_INPUT_INVALID','message','Tool approval inspection envelope is invalid','status',422));
    END IF;
    SELECT *,clock_timestamp() AS database_now INTO run_row FROM __forest_module_schema__.agent_runs
    WHERE bot_id=requested_bot AND run_id=requested_run FOR UPDATE;
    IF NOT FOUND OR run_row.owner_id IS DISTINCT FROM requested_owner
       OR run_row.owner_epoch IS DISTINCT FROM requested_epoch
       OR run_row.owner_expires_at IS NULL OR run_row.owner_expires_at<=run_row.database_now THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,'error',jsonb_build_object(
            'code','STALE_AGENT_RUN_OWNER','message','Agent Run owner is stale','status',409));
    END IF;
    SELECT * INTO tool_row FROM __forest_module_schema__.tool_calls
    WHERE bot_id=requested_bot AND tool_call_id=requested_tool FOR UPDATE;
    IF NOT FOUND OR tool_row.run_id IS DISTINCT FROM requested_run
       OR run_row.current_tool_call_id IS DISTINCT FROM requested_tool
       OR run_row.state IS DISTINCT FROM 'WAIT_APPROVAL'
       OR tool_row.state IS DISTINCT FROM 'WAIT_APPROVAL'
       OR tool_row.approval_ref IS DISTINCT FROM approval_value->>'approval_id'
       OR approval_value->>'plan_digest' IS DISTINCT FROM tool_row.tool_binding_digest THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,'error',jsonb_build_object(
            'code','AGENT_TOOL_APPROVAL_BINDING_CONFLICT','message','Approval differs from the waiting ToolCall','status',409));
    END IF;
    IF approval_state IN ('REJECTED','EXPIRED') THEN
        INSERT INTO __forest_module_schema__.tool_observations(
            bot_id,observation_operation_id,tool_call_id,input_digest,observed_state,evidence_kind,
            approval_ref,error_code,accepted,previous_state,resulting_state)
        VALUES(requested_bot,requested_observation,requested_tool,requested_digest,'CANCELLED',
            'AUTHORIZATION',approval_value->>'approval_id',
            CASE approval_state WHEN 'REJECTED' THEN 'AGENT_TOOL_APPROVAL_REJECTED' ELSE 'AGENT_TOOL_APPROVAL_EXPIRED' END,
            true,'WAIT_APPROVAL','CANCELLED');
        UPDATE __forest_module_schema__.tool_calls SET state='CANCELLED',
            error_code=CASE approval_state WHEN 'REJECTED' THEN 'AGENT_TOOL_APPROVAL_REJECTED' ELSE 'AGENT_TOOL_APPROVAL_EXPIRED' END,
            updated_at=clock_timestamp()
        WHERE bot_id=requested_bot AND tool_call_id=requested_tool;
        UPDATE __forest_module_schema__.agent_runs SET state='PREPARING_MODEL',current_tool_call_id=NULL,
            updated_at=clock_timestamp() WHERE bot_id=requested_bot AND run_id=requested_run;
        resulting_state := 'PREPARING_MODEL';
    ELSE
        resulting_state := 'WAIT_APPROVAL';
    END IF;
    RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',true,'value',jsonb_build_object(
        'protocol','forest.agent-tool-approval/1','tool_call_id',requested_tool,
        'approval_ref',approval_value->>'approval_id','approval_state',approval_state,'resulting_state',resulting_state));
END;
$$;

CREATE FUNCTION agent_prepare_tool_effect(context jsonb, envelope jsonb)
RETURNS jsonb LANGUAGE plpgsql VOLATILE SET search_path = pg_catalog AS $$
DECLARE
    body jsonb := envelope->'input'; owner_value jsonb := body->'owner';
    effect_value jsonb := envelope->'prior'->(body->>'effect_step_id');
    input_value jsonb := effect_value->'input_payload'; wire_value jsonb := effect_value->'wire_payload';
    permit_value jsonb := effect_value->'permit'; requested_bot text := context->>'bot_id';
    requested_run text := owner_value->>'run_id'; requested_owner text := owner_value->>'owner_id';
    requested_epoch bigint := (owner_value->>'epoch')::bigint;
    requested_tool text := body->>'tool_call_id'; requested_effect text := body->>'effect_operation_ref';
    requested_attempt text := body->>'effect_attempt_id'; requested_digest text := body->>'command_digest';
    approval_step text := body->>'approval_step_id'; approval_value jsonb := envelope->'prior'->approval_step;
    run_row record; tool_row record;
BEGIN
    IF envelope->>'protocol' IS DISTINCT FROM 'forest.uow-step/1'
       OR envelope#>>'{caller,component_id}' IS DISTINCT FROM 'agent.runtime'
       OR body->>'effect_step_id' IS DISTINCT FROM 'effect'
       OR effect_value->>'protocol' IS DISTINCT FROM 'forest.effect-command-permit/1'
       OR input_value->>'owner_component_id' IS DISTINCT FROM 'kernel.effect'
       OR input_value->>'owner_kind' IS DISTINCT FROM 'effect.input'
       OR input_value->>'owner_id' IS DISTINCT FROM requested_effect
       OR wire_value->>'owner_component_id' IS DISTINCT FROM 'kernel.effect'
       OR wire_value->>'owner_kind' IS DISTINCT FROM 'effect.wire'
       OR wire_value->>'owner_id' IS DISTINCT FROM requested_attempt
       OR wire_value->>'digest' IS DISTINCT FROM requested_digest
       OR permit_value->>'protocol' IS DISTINCT FROM 'forest.effect-permit/1'
       OR permit_value->>'effect_operation_id' IS DISTINCT FROM requested_effect
       OR permit_value->>'attempt_id' IS DISTINCT FROM requested_attempt
       OR permit_value->>'wire_payload_id' IS DISTINCT FROM wire_value->>'payload_id'
       OR permit_value->>'wire_payload_digest' IS DISTINCT FROM wire_value->>'digest'
       OR (approval_step IS NOT NULL AND (
            approval_step IS DISTINCT FROM 'approval'
            OR approval_value->>'protocol' IS DISTINCT FROM 'forest.approval-state/1'
            OR approval_value->>'request_kind' IS DISTINCT FROM 'agent.tool'
            OR approval_value->>'request_id' IS DISTINCT FROM requested_tool
            OR approval_value->>'action' IS DISTINCT FROM 'tool.invoke'
            OR approval_value->>'input_digest' IS DISTINCT FROM requested_digest
            OR approval_value->>'state' IS DISTINCT FROM 'CONSUMED')) THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,'error',jsonb_build_object(
            'code','AGENT_TOOL_EFFECT_INPUT_INVALID','message','Tool Effect preparation envelope is invalid','status',422));
    END IF;
    SELECT *,clock_timestamp() AS database_now INTO run_row
    FROM __forest_module_schema__.agent_runs
    WHERE bot_id=requested_bot AND run_id=requested_run FOR UPDATE;
    IF NOT FOUND OR run_row.owner_id IS DISTINCT FROM requested_owner
       OR run_row.owner_epoch IS DISTINCT FROM requested_epoch
       OR run_row.owner_expires_at IS NULL OR run_row.owner_expires_at<=run_row.database_now THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,'error',jsonb_build_object(
            'code','STALE_AGENT_RUN_OWNER','message','Agent Run owner is stale','status',409));
    END IF;
    SELECT * INTO tool_row FROM __forest_module_schema__.tool_calls
    WHERE bot_id=requested_bot AND tool_call_id=requested_tool FOR UPDATE;
    IF NOT FOUND OR tool_row.run_id IS DISTINCT FROM requested_run
       OR run_row.current_tool_call_id IS DISTINCT FROM requested_tool
       OR run_row.state IS DISTINCT FROM (CASE WHEN approval_step IS NULL THEN 'WAIT_TOOL' ELSE 'WAIT_APPROVAL' END)
       OR tool_row.effect_operation_ref IS DISTINCT FROM requested_effect
       OR permit_value->>'executor_component_id' IS DISTINCT FROM tool_row.tool_component_id
       OR permit_value->>'executor_generation_id' IS DISTINCT FROM tool_row.tool_generation_id THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,'error',jsonb_build_object(
            'code','AGENT_TOOL_EFFECT_BINDING_CONFLICT','message','Tool Effect differs from the current frozen ToolCall','status',409));
    END IF;
    IF tool_row.approval_required IS DISTINCT FROM (approval_step IS NOT NULL)
       OR (approval_step IS NOT NULL AND (
            tool_row.approval_ref IS DISTINCT FROM approval_value->>'approval_id'
            OR approval_value->>'plan_digest' IS DISTINCT FROM tool_row.tool_binding_digest)) THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,'error',jsonb_build_object(
            'code','AGENT_TOOL_APPROVAL_CONFLICT','message','Tool approval does not match Effect preparation','status',409));
    END IF;
    IF tool_row.effect_attempt_id IS NOT NULL THEN
        IF tool_row.effect_attempt_id IS DISTINCT FROM requested_attempt
           OR tool_row.authorization_ticket_id IS DISTINCT FROM effect_value->>'authorization_ticket_id'
           OR tool_row.wire_payload_id IS DISTINCT FROM wire_value->>'payload_id'
           OR tool_row.wire_payload_digest IS DISTINCT FROM wire_value->>'digest' THEN
            RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,'error',jsonb_build_object(
                'code','AGENT_TOOL_EFFECT_PREPARATION_CONFLICT','message','Tool Effect preparation identity differs','status',409));
        END IF;
    ELSIF tool_row.state IS DISTINCT FROM (CASE WHEN approval_step IS NULL THEN 'PROPOSED' ELSE 'WAIT_APPROVAL' END) THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,'error',jsonb_build_object(
            'code','AGENT_TOOL_EFFECT_BOUNDARY_CLOSED','message','ToolCall is not at the preparation boundary','status',409));
    ELSE
        UPDATE __forest_module_schema__.tool_calls
        SET effect_attempt_id=requested_attempt,
            authorization_ticket_id=effect_value->>'authorization_ticket_id',
            wire_payload_id=wire_value->>'payload_id',wire_payload_digest=wire_value->>'digest',
            updated_at=clock_timestamp()
        WHERE bot_id=requested_bot AND tool_call_id=requested_tool;
    END IF;
    RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',true,'value',jsonb_build_object(
        'protocol','forest.agent-tool-effect-prepared/1','tool_call_id',requested_tool,
        'effect_operation_ref',requested_effect,'effect_attempt_id',requested_attempt,
        'state','PREPARED','authorization_ticket_id',effect_value->>'authorization_ticket_id',
        'input_payload',input_value,'wire_payload',wire_value,'permit',permit_value));
END;
$$;

CREATE FUNCTION agent_mark_tool_handoff(context jsonb, envelope jsonb)
RETURNS jsonb LANGUAGE plpgsql VOLATILE SET search_path = pg_catalog AS $$
DECLARE
    body jsonb := envelope->'input'; owner_value jsonb := body->'owner';
    effect_value jsonb := envelope->'prior'->(body->>'effect_step_id');
    requested_bot text := context->>'bot_id'; requested_run text := owner_value->>'run_id';
    requested_owner text := owner_value->>'owner_id'; requested_epoch bigint := (owner_value->>'epoch')::bigint;
    requested_operation text := body->>'observation_operation_id'; requested_tool text := body->>'tool_call_id';
    requested_attempt text := body->>'effect_attempt_id'; digest_value text;
    run_row record; tool_row record; replay record;
BEGIN
    IF envelope->>'protocol' IS DISTINCT FROM 'forest.uow-step/1'
       OR envelope#>>'{caller,component_id}' IS DISTINCT FROM 'agent.runtime'
       OR body->>'effect_step_id' IS DISTINCT FROM 'effect'
       OR effect_value->>'state' IS DISTINCT FROM 'DISPATCHING'
       OR effect_value->>'attempt_id' IS DISTINCT FROM requested_attempt THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,'error',jsonb_build_object(
            'code','AGENT_TOOL_HANDOFF_INPUT_INVALID','message','Tool handoff envelope is invalid','status',422));
    END IF;
    digest_value := 'sha256:' || encode(sha256(convert_to(jsonb_build_object(
        'tool_call_id',requested_tool,'effect_attempt_id',requested_attempt,
        'effect_operation_id',effect_value->>'effect_operation_id',
        'wire_payload_digest',effect_value->>'wire_payload_digest')::text,'UTF8')),'hex');
    SELECT *,clock_timestamp() AS database_now INTO run_row FROM __forest_module_schema__.agent_runs
    WHERE bot_id=requested_bot AND run_id=requested_run FOR UPDATE;
    IF NOT FOUND OR run_row.owner_id IS DISTINCT FROM requested_owner
       OR run_row.owner_epoch IS DISTINCT FROM requested_epoch
       OR run_row.owner_expires_at IS NULL OR run_row.owner_expires_at<=run_row.database_now THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,'error',jsonb_build_object(
            'code','STALE_AGENT_RUN_OWNER','message','Agent Run owner is stale','status',409));
    END IF;
    PERFORM pg_advisory_xact_lock(hashtextextended(
        'forest.agent.runtime/tool-handoff/' || requested_bot || '/' || requested_operation,0));
    SELECT * INTO replay FROM __forest_module_schema__.tool_observations
    WHERE bot_id=requested_bot AND observation_operation_id=requested_operation;
    IF FOUND THEN
        IF replay.input_digest IS DISTINCT FROM digest_value THEN
            RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,'error',jsonb_build_object(
                'code','TOOL_OBSERVATION_CONFLICT','message','Tool handoff input differs','status',409));
        END IF;
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',true,'value',jsonb_build_object(
            'observation_operation_id',replay.observation_operation_id,'tool_call_id',replay.tool_call_id,
            'observed_state',replay.observed_state,'evidence_kind',replay.evidence_kind,
            'accepted',replay.accepted,'previous_state',replay.previous_state,
            'resulting_state',replay.resulting_state,'conflict_code',replay.conflict_code));
    END IF;
    IF EXISTS (SELECT 1 FROM __forest_module_schema__.run_cancel_requests
        WHERE bot_id=requested_bot AND run_id=requested_run) THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,'error',jsonb_build_object(
            'code','AGENT_CANCEL_REQUESTED','message','Agent cancellation won the tool handoff race','status',409));
    END IF;
    SELECT * INTO tool_row FROM __forest_module_schema__.tool_calls
    WHERE bot_id=requested_bot AND tool_call_id=requested_tool FOR UPDATE;
    IF NOT FOUND OR tool_row.run_id IS DISTINCT FROM requested_run
       OR run_row.current_tool_call_id IS DISTINCT FROM requested_tool
       OR run_row.state IS DISTINCT FROM (CASE WHEN tool_row.approval_required THEN 'WAIT_APPROVAL' ELSE 'WAIT_TOOL' END)
       OR tool_row.state IS DISTINCT FROM (CASE WHEN tool_row.approval_required THEN 'WAIT_APPROVAL' ELSE 'PROPOSED' END)
       OR (tool_row.approval_required AND tool_row.approval_ref IS NULL)
       OR tool_row.effect_attempt_id IS DISTINCT FROM requested_attempt
       OR tool_row.effect_operation_ref IS DISTINCT FROM effect_value->>'effect_operation_id'
       OR tool_row.wire_payload_id IS DISTINCT FROM effect_value->>'wire_payload_id'
       OR tool_row.wire_payload_digest IS DISTINCT FROM effect_value->>'wire_payload_digest'
       OR tool_row.tool_component_id IS DISTINCT FROM effect_value->>'executor_component_id'
       OR tool_row.tool_generation_id IS DISTINCT FROM effect_value->>'executor_generation_id' THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,'error',jsonb_build_object(
            'code','AGENT_TOOL_HANDOFF_CONFLICT','message','Effect handoff differs from the prepared ToolCall','status',409));
    END IF;
    INSERT INTO __forest_module_schema__.tool_observations(
        bot_id,observation_operation_id,tool_call_id,input_digest,observed_state,evidence_kind,
        effect_observation_ref,accepted,previous_state,resulting_state)
    VALUES(requested_bot,requested_operation,requested_tool,digest_value,'DISPATCHING','EXECUTOR',
        requested_attempt,true,tool_row.state,'DISPATCHING');
    UPDATE __forest_module_schema__.tool_calls SET state='DISPATCHING',updated_at=clock_timestamp()
    WHERE bot_id=requested_bot AND tool_call_id=requested_tool;
    IF run_row.state='WAIT_APPROVAL' THEN
        UPDATE __forest_module_schema__.agent_runs SET state='WAIT_TOOL',updated_at=clock_timestamp()
        WHERE bot_id=requested_bot AND run_id=requested_run;
    END IF;
    RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',true,'value',jsonb_build_object(
        'observation_operation_id',requested_operation,'tool_call_id',requested_tool,
        'observed_state','DISPATCHING','evidence_kind','EXECUTOR','accepted',true,
        'previous_state',tool_row.state,'resulting_state','DISPATCHING','conflict_code',NULL));
END;
$$;

CREATE FUNCTION agent_observe_tool_result(context jsonb, envelope jsonb)
RETURNS jsonb LANGUAGE plpgsql VOLATILE SET search_path = pg_catalog AS $$
DECLARE
    body jsonb := envelope->'input'; owner_value jsonb := body->'owner';
    result_value jsonb := body#>'{results,0}'; requested_bot text := context->>'bot_id';
    effect_command_value jsonb := envelope->'prior'->(body->>'effect_step_id');
    effect_evidence jsonb := effect_command_value->'evidence_payload';
    effect_observation jsonb := effect_command_value->'observation';
    effect_parents jsonb := effect_command_value->'evidence_parents';
    requested_run text := owner_value->>'run_id'; requested_owner text := owner_value->>'owner_id';
    requested_epoch bigint := (owner_value->>'epoch')::bigint;
    requested_operation text := body->>'observation_operation_id'; requested_tool text := body->>'tool_call_id';
    requested_attempt text := body->>'effect_attempt_id';
    observed_value text := body->>'observed_state'; evidence_value text := body->>'evidence_kind';
    effect_value text := body->>'effect_observation_ref'; approval_value text := body->>'approval_ref';
    error_value text := body->>'error_code'; digest_value text; run_row record; tool_row record; replay record;
    previous_value text; resulting_value text; accepted_value boolean := false;
    conflict_value text := NULL; run_state_value text; terminal_value boolean;
BEGIN
    IF jsonb_array_length(body->'results')=0 THEN result_value := NULL; END IF;
    IF envelope->>'protocol' IS DISTINCT FROM 'forest.uow-step/1'
       OR envelope#>>'{caller,component_id}' IS DISTINCT FROM 'agent.runtime'
       OR (observed_value='WAIT_APPROVAL' AND approval_value IS NULL)
       OR (observed_value IN ('DISPATCHING','SUCCEEDED','FAILED','CANCELLED','UNKNOWN') AND effect_value IS NULL)
       OR (observed_value='SUCCEEDED' AND (result_value IS NULL OR error_value IS NOT NULL))
       OR (observed_value IN ('FAILED','CANCELLED') AND error_value IS NULL)
       OR (observed_value NOT IN ('SUCCEEDED','FAILED','CANCELLED') AND (result_value IS NOT NULL OR error_value IS NOT NULL))
       OR (result_value IS NOT NULL AND (result_value->>'owner_kind' IS DISTINCT FROM 'tool.result'
            OR result_value->>'owner_id' IS DISTINCT FROM requested_tool)) THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,'error',jsonb_build_object(
            'code','AGENT_TOOL_OBSERVATION_INPUT_INVALID','message','Tool observation envelope is invalid','status',422));
    END IF;
    digest_value := 'sha256:' || encode(sha256(convert_to(jsonb_build_object(
        'tool_call_id',requested_tool,'observed_state',observed_value,'evidence_kind',evidence_value,
        'effect_observation_ref',effect_value,'approval_ref',approval_value,'result',result_value,
        'error_code',error_value)::text,'UTF8')),'hex');
    SELECT *,clock_timestamp() AS database_now INTO run_row FROM __forest_module_schema__.agent_runs
    WHERE bot_id=requested_bot AND run_id=requested_run FOR UPDATE;
    IF NOT FOUND OR run_row.owner_id IS DISTINCT FROM requested_owner OR run_row.owner_epoch IS DISTINCT FROM requested_epoch
       OR run_row.owner_expires_at IS NULL OR run_row.owner_expires_at<=run_row.database_now THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,'error',jsonb_build_object(
            'code','STALE_AGENT_RUN_OWNER','message','Agent Run owner is stale','status',409));
    END IF;
    PERFORM pg_advisory_xact_lock(hashtextextended('forest.agent.runtime/tool-observation/' || requested_bot || '/' || requested_operation,0));
    SELECT * INTO replay FROM __forest_module_schema__.tool_observations
    WHERE bot_id=requested_bot AND observation_operation_id=requested_operation;
    IF FOUND THEN
        IF replay.input_digest IS DISTINCT FROM digest_value THEN
            RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,'error',jsonb_build_object(
                'code','TOOL_OBSERVATION_CONFLICT','message','Tool observation input differs','status',409));
        END IF;
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',true,'value',jsonb_build_object(
            'observation_operation_id',replay.observation_operation_id,'tool_call_id',replay.tool_call_id,
            'observed_state',replay.observed_state,'evidence_kind',replay.evidence_kind,
            'accepted',replay.accepted,'previous_state',replay.previous_state,
            'resulting_state',replay.resulting_state,'conflict_code',replay.conflict_code));
    END IF;
    IF observed_value='DISPATCHING' AND EXISTS (SELECT 1 FROM __forest_module_schema__.run_cancel_requests
        WHERE bot_id=requested_bot AND run_id=requested_run) THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,'error',jsonb_build_object(
            'code','AGENT_CANCEL_REQUESTED','message','Agent cancellation won the tool handoff race','status',409));
    END IF;
    SELECT * INTO tool_row FROM __forest_module_schema__.tool_calls
    WHERE bot_id=requested_bot AND tool_call_id=requested_tool FOR UPDATE;
    IF NOT FOUND OR tool_row.run_id IS DISTINCT FROM requested_run THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,'error',jsonb_build_object(
            'code','AGENT_TOOL_NOT_FOUND','message','Tool call does not exist','status',404));
    END IF;
    IF run_row.current_tool_call_id IS DISTINCT FROM requested_tool THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,'error',jsonb_build_object(
            'code','AGENT_TOOL_STALE','message','Tool call is no longer current','status',409));
    END IF;
    IF result_value IS NOT NULL AND result_value->>'owner_component_id' IS DISTINCT FROM tool_row.tool_component_id THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,'error',jsonb_build_object(
            'code','TOOL_RESULT_IDENTITY_CONFLICT','message','Tool result owner differs from frozen binding','status',409));
    END IF;
    previous_value := tool_row.state;
    IF previous_value IN ('DISPATCHING','UNKNOWN') AND (
          body->>'effect_step_id' IS DISTINCT FROM 'effect'
          OR body->>'evidence_digest' IS NULL
          OR requested_attempt IS DISTINCT FROM tool_row.effect_attempt_id
          OR effect_command_value->>'protocol' IS DISTINCT FROM 'forest.effect-command-observation/1'
          OR effect_evidence->>'owner_component_id' IS DISTINCT FROM 'kernel.effect'
          OR effect_evidence->>'owner_kind' IS DISTINCT FROM 'effect.evidence'
          OR effect_evidence->>'owner_id' IS DISTINCT FROM requested_operation
          OR effect_evidence->>'digest' IS DISTINCT FROM body->>'evidence_digest'
          OR effect_observation->>'attempt_id' IS DISTINCT FROM requested_attempt
          OR effect_observation->>'observation_operation_id' IS DISTINCT FROM requested_operation
          OR lower(effect_observation->>'evidence_kind') IS DISTINCT FROM lower(evidence_value)
          OR (effect_observation->>'accepted')::boolean IS DISTINCT FROM true
          OR effect_observation->>'resulting_state' IS DISTINCT FROM
             CASE WHEN observed_value='UNKNOWN' THEN 'UNKNOWN' ELSE 'SUCCEEDED' END
          OR jsonb_typeof(effect_parents) IS DISTINCT FROM 'array'
          OR jsonb_array_length(effect_parents) IS DISTINCT FROM jsonb_array_length(body->'results')
          OR EXISTS (SELECT 1 FROM jsonb_array_elements(body->'results') item
              WHERE NOT EXISTS (SELECT 1 FROM jsonb_array_elements(effect_parents) parent
                  WHERE parent=item))
          OR EXISTS (SELECT 1 FROM jsonb_array_elements(effect_parents) parent
              WHERE NOT EXISTS (SELECT 1 FROM jsonb_array_elements(body->'results') item
                  WHERE item=parent))
       ) THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,'error',jsonb_build_object(
            'code','AGENT_TOOL_EFFECT_OBSERVATION_INVALID','message','Tool observation is not backed by the matching Kernel Effect evidence','status',409));
    END IF;
    IF (previous_value='PROPOSED' AND observed_value IN ('WAIT_APPROVAL','DISPATCHING','FAILED','CANCELLED'))
       OR (previous_value='WAIT_APPROVAL' AND observed_value IN ('DISPATCHING','FAILED','CANCELLED'))
       OR (previous_value='DISPATCHING' AND observed_value IN ('SUCCEEDED','FAILED','CANCELLED','UNKNOWN'))
       OR (previous_value='UNKNOWN' AND tool_row.supports_query AND evidence_value='RECONCILIATION'
           AND observed_value IN ('SUCCEEDED','FAILED','CANCELLED')) THEN accepted_value := true;
    ELSE conflict_value := 'TOOL_RESULT_ALREADY_SETTLED'; END IF;
    resulting_value := CASE WHEN accepted_value THEN observed_value ELSE previous_value END;
    INSERT INTO __forest_module_schema__.tool_observations(bot_id,observation_operation_id,tool_call_id,
        input_digest,observed_state,evidence_kind,effect_observation_ref,approval_ref,result_payload_id,
        result_payload_digest,error_code,accepted,previous_state,resulting_state,conflict_code)
    VALUES(requested_bot,requested_operation,requested_tool,digest_value,observed_value,evidence_value,
        effect_value,approval_value,result_value->>'payload_id',result_value->>'digest',error_value,
        accepted_value,previous_value,resulting_value,conflict_value);
    IF accepted_value THEN
        UPDATE __forest_module_schema__.tool_calls SET state=observed_value,
            approval_ref=COALESCE(approval_value,approval_ref),result_payload_id=result_value->>'payload_id',
            result_payload_digest=result_value->>'digest',error_code=error_value,updated_at=clock_timestamp()
        WHERE bot_id=requested_bot AND tool_call_id=requested_tool;
        run_state_value := CASE observed_value WHEN 'WAIT_APPROVAL' THEN 'WAIT_APPROVAL'
            WHEN 'DISPATCHING' THEN 'WAIT_TOOL' WHEN 'UNKNOWN' THEN 'BLOCKED_UNKNOWN'
            ELSE 'PREPARING_MODEL' END;
        terminal_value := observed_value IN ('SUCCEEDED','FAILED','CANCELLED');
        UPDATE __forest_module_schema__.agent_runs SET state=run_state_value,
            current_tool_call_id=CASE WHEN terminal_value THEN NULL ELSE requested_tool END,
            updated_at=clock_timestamp() WHERE bot_id=requested_bot AND run_id=requested_run;
    END IF;
    RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',true,'value',jsonb_build_object(
        'observation_operation_id',requested_operation,'tool_call_id',requested_tool,
        'observed_state',observed_value,'evidence_kind',evidence_value,'accepted',accepted_value,
        'previous_state',previous_value,'resulting_state',resulting_value,'conflict_code',conflict_value));
END;
$$;

CREATE FUNCTION agent_observe_tool_effect(context jsonb, envelope jsonb)
RETURNS jsonb LANGUAGE sql VOLATILE SET search_path = pg_catalog AS $$
    SELECT __forest_module_schema__.agent_observe_tool_result(context,envelope)
$$;

CREATE FUNCTION agent_finalize_candidate(context jsonb, envelope jsonb)
RETURNS jsonb
LANGUAGE plpgsql
VOLATILE
SET search_path = pg_catalog
AS $$
DECLARE
    body jsonb := envelope->'input';
    owner_value jsonb := body->'owner';
    source_value jsonb := body->'source_candidate';
    candidate_value jsonb := body->'candidate';
    requested_bot text := context->>'bot_id';
    requested_run text := owner_value->>'run_id';
    requested_owner text := owner_value->>'owner_id';
    requested_epoch bigint := (owner_value->>'epoch')::bigint;
    requested_turn text := body->>'turn_id';
    run_row record;
    turn_row record;
BEGIN
    IF envelope->>'protocol' IS DISTINCT FROM 'forest.uow-step/1'
       OR envelope#>>'{caller,component_id}' IS DISTINCT FROM 'agent.runtime'
       OR envelope->'prior' IS DISTINCT FROM '{}'::jsonb
       OR source_value->>'owner_component_id' IS DISTINCT FROM 'agent.runtime'
       OR source_value->>'owner_kind' IS DISTINCT FROM 'assistant.candidate'
       OR source_value->>'owner_id' IS DISTINCT FROM requested_turn
       OR candidate_value->>'owner_component_id' IS DISTINCT FROM 'agent.runtime'
       OR candidate_value->>'owner_kind' IS DISTINCT FROM 'final.reply' THEN
        RETURN jsonb_build_object(
            'protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object(
                'code','AGENT_FINAL_CANDIDATE_INPUT_INVALID',
                'message','Final candidate envelope is invalid','status',422
            )
        );
    END IF;
    SELECT *,clock_timestamp() AS database_now INTO run_row
    FROM __forest_module_schema__.agent_runs
    WHERE bot_id=requested_bot AND run_id=requested_run FOR UPDATE;
    IF NOT FOUND OR run_row.owner_id IS DISTINCT FROM requested_owner
       OR run_row.owner_epoch IS DISTINCT FROM requested_epoch
       OR run_row.owner_expires_at IS NULL
       OR run_row.owner_expires_at<=run_row.database_now THEN
        RETURN jsonb_build_object(
            'protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object(
                'code','STALE_AGENT_RUN_OWNER','message','Agent Run owner is stale','status',409
            )
        );
    END IF;
    SELECT * INTO turn_row
    FROM __forest_module_schema__.model_turns
    WHERE bot_id=requested_bot AND run_id=requested_run
    ORDER BY step_seq DESC LIMIT 1;
    IF turn_row.turn_id IS NULL
       OR turn_row.turn_id IS DISTINCT FROM requested_turn
       OR turn_row.terminal IS DISTINCT FROM true
       OR turn_row.assistant_payload_id IS DISTINCT FROM source_value->>'payload_id'
       OR turn_row.assistant_payload_digest IS DISTINCT FROM source_value->>'digest' THEN
        RETURN jsonb_build_object(
            'protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object(
                'code','AGENT_FINAL_CANDIDATE_CONFLICT',
                'message','Final candidate is not the latest terminal ModelTurn','status',409
            )
        );
    END IF;
    IF run_row.state='FINAL_READY' THEN
        IF run_row.final_candidate_payload_id IS DISTINCT FROM candidate_value->>'payload_id'
           OR run_row.final_candidate_payload_digest IS DISTINCT FROM candidate_value->>'digest' THEN
            RETURN jsonb_build_object(
                'protocol','forest.domain-result/1','ok',false,
                'error',jsonb_build_object(
                    'code','AGENT_FINAL_CANDIDATE_CONFLICT',
                    'message','Run already froze another final candidate','status',409
                )
            );
        END IF;
        RETURN __forest_module_schema__.agent_run_snapshot(
            context,jsonb_build_object('run_id',requested_run)
        );
    END IF;
    IF run_row.state IS DISTINCT FROM 'SAFE_BOUNDARY' THEN
        RETURN jsonb_build_object(
            'protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object(
                'code','AGENT_FINAL_CANDIDATE_BOUNDARY_CLOSED',
                'message','Run is not at a terminal model-safe boundary','status',409
            )
        );
    END IF;
    UPDATE __forest_module_schema__.agent_runs
    SET state='FINAL_READY',
        final_candidate_payload_id=candidate_value->>'payload_id',
        final_candidate_payload_digest=candidate_value->>'digest',
        updated_at=clock_timestamp()
    WHERE bot_id=requested_bot AND run_id=requested_run;
    RETURN __forest_module_schema__.agent_run_snapshot(
        context,jsonb_build_object('run_id',requested_run)
    );
END;
$$;

CREATE FUNCTION agent_claim_input_valid(batch jsonb)
RETURNS boolean LANGUAGE sql IMMUTABLE SET search_path = pg_catalog AS $$
    SELECT COALESCE(
        batch#>>'{trigger_claim,ref}' ~ '^claim:[a-f0-9]{64}$'
        AND (batch#>>'{trigger_claim,count}')::integer>0
        AND (batch#>>'{trigger_claim,last_trigger_ingress_seq}')::bigint
            <=(batch->>'selected_input_end_seq')::bigint
        AND batch#>>'{trigger_claim,digest}' ~ '^sha256:[a-f0-9]{64}$',
        false
    )
$$;

CREATE FUNCTION agent_start_run(context jsonb, envelope jsonb)
RETURNS jsonb
LANGUAGE plpgsql
VOLATILE
SET search_path = pg_catalog
AS $$
DECLARE
    body jsonb := envelope->'input';
    batch jsonb := body->'input_batch';
    requested_bot text := context->>'bot_id';
    requested_operation text := context->>'operation_id';
    requested_digest text := context->>'participant_input_digest';
    requested_conversation text := body->>'conversation_ref';
    requested_session text := body->>'session_id';
    requested_run text := body->>'run_id';
    deadline_value timestamptz;
    now_value timestamptz;
    existing record;
BEGIN
    IF envelope->>'protocol' IS DISTINCT FROM 'forest.uow-step/1'
       OR envelope#>>'{caller,component_id}' IS DISTINCT FROM 'agent.runtime'
       OR requested_operation IS NULL
       OR requested_digest !~ '^sha256:[a-f0-9]{64}$'
       OR body#>>'{config,owner_component_id}' IS DISTINCT FROM 'agent.runtime'
       OR body#>>'{config,owner_kind}' IS DISTINCT FROM 'module.configuration'
       OR body#>>'{preset,owner_component_id}' IS DISTINCT FROM 'agent.runtime'
       OR body#>>'{preset,owner_kind}' IS DISTINCT FROM 'preset.context' THEN
        RETURN jsonb_build_object(
            'protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object(
                'code','AGENT_RUNTIME_INPUT_INVALID',
                'message','Agent Run start envelope is invalid','status',422
            )
        );
    END IF;
    BEGIN
        deadline_value := (body->>'absolute_deadline')::timestamptz;
    EXCEPTION WHEN invalid_datetime_format OR datetime_field_overflow THEN
        RETURN jsonb_build_object(
            'protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object(
                'code','AGENT_RUNTIME_INPUT_INVALID',
                'message','Agent Run deadline is invalid','status',422
            )
        );
    END;
    IF NOT __forest_module_schema__.agent_claim_input_valid(batch) THEN
        RETURN jsonb_build_object(
            'protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object(
                'code','AGENT_RUNTIME_INPUT_INVALID',
                'message','Agent input trigger claim is invalid','status',422
            )
        );
    END IF;
    PERFORM pg_advisory_xact_lock(hashtextextended(
        'forest.agent.runtime/start/' || requested_bot || '/' || requested_operation,0
    ));
    SELECT * INTO existing
    FROM __forest_module_schema__.agent_runs
    WHERE bot_id=requested_bot
      AND (start_operation_id=requested_operation OR run_id=requested_run)
    FOR UPDATE;
    IF FOUND THEN
        IF existing.start_operation_id IS DISTINCT FROM requested_operation
           OR existing.input_digest IS DISTINCT FROM requested_digest THEN
            RETURN jsonb_build_object(
                'protocol','forest.domain-result/1','ok',false,
                'error',jsonb_build_object(
                    'code','AGENT_START_OPERATION_CONFLICT',
                    'message','Agent start input differs','status',409
                )
            );
        END IF;
        RETURN __forest_module_schema__.agent_run_snapshot(
            context,jsonb_build_object('run_id',requested_run)
        );
    END IF;
    PERFORM pg_advisory_xact_lock(hashtextextended(
        'forest.agent.runtime/conversation/' || requested_bot || '/' || requested_conversation,0
    ));
    SELECT clock_timestamp() INTO now_value;
    IF deadline_value<=now_value OR deadline_value>now_value+interval '24 hours' THEN
        RETURN jsonb_build_object(
            'protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object(
                'code','AGENT_RUNTIME_INPUT_INVALID',
                'message','Run deadline is outside its bound','status',422
            )
        );
    END IF;
    IF EXISTS (
        SELECT 1 FROM __forest_module_schema__.agent_sessions
        WHERE bot_id=requested_bot
          AND (session_id=requested_session OR (conversation_ref=requested_conversation AND state='ACTIVE'))
    ) THEN
        RETURN jsonb_build_object(
            'protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object(
                'code','AGENT_SESSION_CONFLICT',
                'message','Conversation already has an Agent Session','status',409
            )
        );
    END IF;
    INSERT INTO __forest_module_schema__.agent_sessions(
        bot_id,session_id,conversation_ref,start_operation_id,input_digest,active_run_id
    ) VALUES (
        requested_bot,requested_session,requested_conversation,
        requested_operation,requested_digest,requested_run
    );
    INSERT INTO __forest_module_schema__.agent_runs(
        bot_id,run_id,session_id,start_operation_id,input_digest,state,
        agent_spec_revision,config_payload_id,config_payload_digest,
        config_owner_component_id,config_owner_kind,config_owner_id,
        preset_revision,preset_payload_id,preset_payload_digest,
        preset_owner_component_id,preset_owner_kind,preset_owner_id,
        core_protocol_id,core_protocol_version,core_prompt_digest,parser_revision,
        absolute_deadline,input_revision,selected_input_end_seq,
        context_revision,context_view_revision
    ) VALUES (
        requested_bot,requested_run,requested_session,requested_operation,requested_digest,
        'PREPARING_MODEL',body->>'agent_spec_revision',body#>>'{config,payload_id}',
        body#>>'{config,digest}',body#>>'{config,owner_component_id}',
        body#>>'{config,owner_kind}',body#>>'{config,owner_id}',body->>'preset_revision',
        body#>>'{preset,payload_id}',body#>>'{preset,digest}',
        body#>>'{preset,owner_component_id}',body#>>'{preset,owner_kind}',
        body#>>'{preset,owner_id}','forest-agent-protocol',1,
        'sha256:12e2de79a35becbe0b62687bdb7a26aa7c4eb91134c5ed1770ddc93828891f8c',
        'forest-model-turn-xml-v1.2',deadline_value,1,
        (batch->>'selected_input_end_seq')::bigint,(batch->>'context_revision')::bigint,
        (batch->>'context_view_revision')::bigint
    );
    INSERT INTO __forest_module_schema__.run_inputs(
        bot_id,run_id,input_revision,input_batch_ref,input_batch_digest,
        selected_input_end_seq,context_revision,context_view_revision,
        source_manifest_digest,trigger_claim_ref,trigger_claim_digest,
        trigger_count,last_trigger_ref,last_trigger_ingress_seq
    ) VALUES (
        requested_bot,requested_run,1,batch->>'input_batch_ref',batch->>'input_batch_digest',
        (batch->>'selected_input_end_seq')::bigint,(batch->>'context_revision')::bigint,
        (batch->>'context_view_revision')::bigint,batch->>'source_manifest_digest',
        batch#>>'{trigger_claim,ref}',batch#>>'{trigger_claim,digest}',
        (batch#>>'{trigger_claim,count}')::integer,
        batch#>>'{trigger_claim,last_trigger_ref}',
        (batch#>>'{trigger_claim,last_trigger_ingress_seq}')::bigint
    );
    RETURN __forest_module_schema__.agent_run_snapshot(
        context,jsonb_build_object('run_id',requested_run)
    );
END;
$$;

CREATE FUNCTION agent_batch_from_selection(selection jsonb)
RETURNS jsonb LANGUAGE plpgsql IMMUTABLE SET search_path = pg_catalog AS $$
DECLARE
    source_manifest jsonb;
    source_digest text;
BEGIN
    source_manifest := jsonb_build_object(
        'conversation_id',selection->>'conversation_id',
        'input_batch_ref',selection->>'input_batch_id',
        'event_ids',selection->'event_ids',
        'trigger_claim',selection->'trigger_claim',
        'selected_input_end_seq',(selection->>'selected_input_end_seq')::bigint,
        'context_revision',(selection->>'context_revision')::bigint,
        'context_view_revision',(selection->>'context_view_revision')::bigint
    );
    source_digest := 'sha256:' || encode(
        sha256(convert_to(source_manifest::text,'UTF8')),'hex'
    );
    RETURN jsonb_build_object(
        'input_batch_ref',selection->>'input_batch_id',
        'input_batch_digest',source_digest,
        'selected_input_end_seq',(selection->>'selected_input_end_seq')::bigint,
        'context_revision',(selection->>'context_revision')::bigint,
        'context_view_revision',(selection->>'context_view_revision')::bigint,
        'source_manifest_digest',source_digest,
        'trigger_claim',selection->'trigger_claim'
    );
END;
$$;

CREATE FUNCTION agent_start_selected_run(context jsonb, envelope jsonb)
RETURNS jsonb
LANGUAGE plpgsql
VOLATILE
SET search_path = pg_catalog
AS $$
DECLARE
    body jsonb := envelope->'input';
    selection jsonb := envelope->'prior'->(body->>'selection_step_id');
    memory_value jsonb := envelope->'prior'->(body->>'memory_step_id');
    batch_value jsonb;
    start_result jsonb;
    event_count integer;
    qualified_count integer;
    pending_count integer;
BEGIN
    IF envelope->>'protocol' IS DISTINCT FROM 'forest.uow-step/1'
       OR envelope#>>'{caller,component_id}' IS DISTINCT FROM 'agent.runtime'
       OR body->>'selection_step_id' IS DISTINCT FROM 'conversation'
       OR body->>'memory_step_id' IS DISTINCT FROM 'memory'
       OR selection IS NULL OR memory_value IS NULL
       OR memory_value->>'protocol' IS DISTINCT FROM 'forest.memory-input-qualified/1'
       OR selection->>'boundary' IS DISTINCT FROM 'IDLE'
       OR selection->>'conversation_id' IS DISTINCT FROM body->>'conversation_ref'
       OR selection->>'session_ref' IS DISTINCT FROM body->>'session_id'
       OR selection->>'run_ref' IS DISTINCT FROM body->>'run_id'
       OR selection->>'status' NOT IN ('BATCH','IDLE') THEN
        RETURN jsonb_build_object(
            'protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object(
                'code','AGENT_START_SELECTION_INVALID',
                'message','Initial Agent selection is invalid','status',409
            )
        );
    END IF;
    qualified_count := (memory_value->>'qualified_count')::integer;
    pending_count := (memory_value->>'pending_count')::integer;
    IF selection->>'status'='IDLE' THEN
        IF memory_value->>'input_batch_ref' IS NOT NULL
           OR qualified_count<>0 OR pending_count<>0 THEN
            RETURN jsonb_build_object(
                'protocol','forest.domain-result/1','ok',false,
                'error',jsonb_build_object(
                    'code','AGENT_START_SELECTION_CONFLICT',
                    'message','Memory qualification differs from idle selection','status',409
                )
            );
        END IF;
        RETURN jsonb_build_object(
            'protocol','forest.domain-result/1','ok',true,
            'value',jsonb_build_object(
                'protocol','forest.agent-start-selection/1','status','IDLE',
                'run',NULL,'memory_qualified_count',0,'memory_pending_count',0
            )
        );
    END IF;
    IF selection->>'kind' IS DISTINCT FROM 'TRIGGER_INPUT'
       OR selection->>'input_batch_id' IS NULL
       OR jsonb_typeof(selection->'event_ids') IS DISTINCT FROM 'array'
       OR jsonb_typeof(selection->'trigger_claim') IS DISTINCT FROM 'object'
       OR memory_value->>'input_batch_ref' IS DISTINCT FROM selection->>'input_batch_id' THEN
        RETURN jsonb_build_object(
            'protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object(
                'code','AGENT_START_SELECTION_INVALID',
                'message','Initial Agent selection is invalid','status',409
            )
        );
    END IF;
    event_count := jsonb_array_length(selection->'event_ids');
    IF qualified_count < 0 OR pending_count < 0
       OR qualified_count+pending_count<>event_count THEN
        RETURN jsonb_build_object(
            'protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object(
                'code','AGENT_START_SELECTION_CONFLICT',
                'message','Memory qualification does not cover initial input','status',409
            )
        );
    END IF;
    batch_value := __forest_module_schema__.agent_batch_from_selection(selection);
    start_result := __forest_module_schema__.agent_start_run(
        context,
        jsonb_build_object(
            'protocol','forest.uow-step/1','caller',envelope->'caller',
            'input',(body-'selection_step_id'-'memory_step_id')
                || jsonb_build_object('input_batch',batch_value),
            'prior',jsonb_build_object()
        )
    );
    IF start_result->>'ok' IS DISTINCT FROM 'true' THEN
        RETURN start_result;
    END IF;
    RETURN jsonb_build_object(
        'protocol','forest.domain-result/1','ok',true,
        'value',jsonb_build_object(
            'protocol','forest.agent-start-selection/1','status','STARTED',
            'run',start_result->'value','memory_qualified_count',qualified_count,
            'memory_pending_count',pending_count
        )
    );
END;
$$;

CREATE FUNCTION agent_append_run_input(context jsonb, envelope jsonb)
RETURNS jsonb
LANGUAGE plpgsql
VOLATILE
SET search_path = pg_catalog
AS $$
DECLARE
    body jsonb := envelope->'input';
    owner_value jsonb := body->'owner';
    batch jsonb := body->'input_batch';
    requested_bot text := context->>'bot_id';
    requested_run text := owner_value->>'run_id';
    requested_owner text := owner_value->>'owner_id';
    requested_epoch bigint := (owner_value->>'epoch')::bigint;
    run_row record;
    existing record;
    next_revision bigint;
BEGIN
    IF envelope->>'protocol' IS DISTINCT FROM 'forest.uow-step/1'
       OR envelope#>>'{caller,component_id}' IS DISTINCT FROM 'agent.runtime' THEN
        RETURN jsonb_build_object(
            'protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object(
                'code','AGENT_RUNTIME_INPUT_INVALID',
                'message','Agent input append envelope is invalid','status',422
            )
        );
    END IF;
    SELECT *,clock_timestamp() AS database_now INTO run_row
    FROM __forest_module_schema__.agent_runs
    WHERE bot_id=requested_bot AND run_id=requested_run FOR UPDATE;
    IF NOT FOUND OR run_row.owner_id IS DISTINCT FROM requested_owner
       OR run_row.owner_epoch IS DISTINCT FROM requested_epoch
       OR run_row.owner_expires_at IS NULL
       OR run_row.owner_expires_at<=run_row.database_now THEN
        RETURN jsonb_build_object(
            'protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object(
                'code','STALE_AGENT_RUN_OWNER','message','Agent Run owner is stale','status',409
            )
        );
    END IF;
    SELECT * INTO existing
    FROM __forest_module_schema__.run_inputs
    WHERE bot_id=requested_bot AND input_batch_ref=batch->>'input_batch_ref';
    IF FOUND THEN
        IF existing.run_id IS DISTINCT FROM requested_run
           OR existing.input_batch_digest IS DISTINCT FROM batch->>'input_batch_digest'
           OR existing.selected_input_end_seq IS DISTINCT FROM (batch->>'selected_input_end_seq')::bigint
           OR existing.context_revision IS DISTINCT FROM (batch->>'context_revision')::bigint
           OR existing.context_view_revision IS DISTINCT FROM (batch->>'context_view_revision')::bigint
           OR existing.source_manifest_digest IS DISTINCT FROM batch->>'source_manifest_digest'
           OR existing.trigger_claim_ref IS DISTINCT FROM batch#>>'{trigger_claim,ref}'
           OR existing.trigger_claim_digest IS DISTINCT FROM batch#>>'{trigger_claim,digest}'
           OR existing.trigger_count IS DISTINCT FROM (batch#>>'{trigger_claim,count}')::integer
           OR existing.last_trigger_ref IS DISTINCT FROM
                batch#>>'{trigger_claim,last_trigger_ref}'
           OR existing.last_trigger_ingress_seq IS DISTINCT FROM
                (batch#>>'{trigger_claim,last_trigger_ingress_seq}')::bigint THEN
            RETURN jsonb_build_object(
                'protocol','forest.domain-result/1','ok',false,
                'error',jsonb_build_object(
                    'code','AGENT_INPUT_CONFLICT','message','InputBatch identity differs','status',409
                )
            );
        END IF;
        RETURN __forest_module_schema__.agent_run_snapshot(
            context,jsonb_build_object('run_id',requested_run)
        );
    END IF;
    IF run_row.state NOT IN ('WAIT_INPUT','PREPARING_MODEL','SAFE_BOUNDARY','FINAL_READY') THEN
        RETURN jsonb_build_object(
            'protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object(
                'code','AGENT_INPUT_BOUNDARY_CLOSED',
                'message','Run is not at a model-safe input boundary','status',409
            )
        );
    END IF;
    IF run_row.state='PREPARING_MODEL' AND EXISTS (
        SELECT 1 FROM __forest_module_schema__.model_requests
        WHERE bot_id=requested_bot AND run_id=requested_run
          AND step_seq=run_row.next_step_seq
    ) THEN
        RETURN jsonb_build_object(
            'protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object(
                'code','AGENT_INPUT_BOUNDARY_CLOSED',
                'message','The next model request already froze its input','status',409
            )
        );
    END IF;
    IF (batch->>'selected_input_end_seq')::bigint<=run_row.selected_input_end_seq
       OR (batch->>'context_revision')::bigint<=run_row.context_revision
       OR (batch->>'context_view_revision')::bigint<run_row.context_view_revision
       OR NOT __forest_module_schema__.agent_claim_input_valid(batch) THEN
        RETURN jsonb_build_object(
            'protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object(
                'code','AGENT_INPUT_CONFLICT','message','InputBatch does not advance safely','status',409
            )
        );
    END IF;
    next_revision := run_row.input_revision+1;
    INSERT INTO __forest_module_schema__.run_inputs(
        bot_id,run_id,input_revision,input_batch_ref,input_batch_digest,
        selected_input_end_seq,context_revision,context_view_revision,
        source_manifest_digest,trigger_claim_ref,trigger_claim_digest,
        trigger_count,last_trigger_ref,last_trigger_ingress_seq
    ) VALUES (
        requested_bot,requested_run,next_revision,batch->>'input_batch_ref',
        batch->>'input_batch_digest',(batch->>'selected_input_end_seq')::bigint,
        (batch->>'context_revision')::bigint,(batch->>'context_view_revision')::bigint,
        batch->>'source_manifest_digest',
        batch#>>'{trigger_claim,ref}',batch#>>'{trigger_claim,digest}',
        (batch#>>'{trigger_claim,count}')::integer,
        batch#>>'{trigger_claim,last_trigger_ref}',
        (batch#>>'{trigger_claim,last_trigger_ingress_seq}')::bigint
    );
    UPDATE __forest_module_schema__.agent_runs
    SET state='PREPARING_MODEL',input_revision=next_revision,
        selected_input_end_seq=(batch->>'selected_input_end_seq')::bigint,
        context_revision=(batch->>'context_revision')::bigint,
        context_view_revision=(batch->>'context_view_revision')::bigint,
        final_candidate_payload_id=NULL,final_candidate_payload_digest=NULL,
        updated_at=clock_timestamp()
    WHERE bot_id=requested_bot AND run_id=requested_run;
    RETURN __forest_module_schema__.agent_run_snapshot(
        context,jsonb_build_object('run_id',requested_run)
    );
END;
$$;

CREATE FUNCTION agent_accept_selected_input(context jsonb, envelope jsonb)
RETURNS jsonb
LANGUAGE plpgsql
VOLATILE
SET search_path = pg_catalog
AS $$
DECLARE
    body jsonb := envelope->'input';
    prior jsonb := envelope->'prior';
    owner_value jsonb := body->'owner';
    requested_run text := owner_value->>'run_id';
    selection jsonb := prior->(body->>'selection_step_id');
    memory_value jsonb := prior->(body->>'memory_step_id');
    selection_status text;
    batch_value jsonb;
    append_result jsonb;
    snapshot jsonb;
    event_count integer;
    qualified_count integer;
    pending_count integer;
BEGIN
    IF envelope->>'protocol' IS DISTINCT FROM 'forest.uow-step/1'
       OR envelope#>>'{caller,component_id}' IS DISTINCT FROM 'agent.runtime'
       OR body IS NULL OR prior IS NULL OR owner_value IS NULL
       OR selection IS NULL OR memory_value IS NULL
       OR body->>'selection_step_id' IS DISTINCT FROM 'conversation'
       OR body->>'memory_step_id' IS DISTINCT FROM 'memory'
       OR memory_value->>'protocol' IS DISTINCT FROM 'forest.memory-input-qualified/1' THEN
        RETURN jsonb_build_object(
            'protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object(
                'code','AGENT_INPUT_ACQUISITION_INVALID',
                'message','Agent input acquisition envelope is invalid','status',422
            )
        );
    END IF;
    selection_status := selection->>'status';
    qualified_count := (memory_value->>'qualified_count')::integer;
    pending_count := (memory_value->>'pending_count')::integer;
    IF selection_status IS NULL
       OR selection_status NOT IN ('BATCH','WAIT_INPUT','CONTINUE','IDLE')
       OR selection->>'selected_input_end_seq' IS NULL
       OR selection->>'conversation_id' IS NULL
       OR qualified_count < 0 OR pending_count < 0 THEN
        RETURN jsonb_build_object(
            'protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object(
                'code','AGENT_INPUT_ACQUISITION_INVALID',
                'message','Conversation selection is invalid','status',422
            )
        );
    END IF;
    IF selection_status <> 'BATCH' THEN
        IF memory_value->>'input_batch_ref' IS NOT NULL
           OR qualified_count <> 0 OR pending_count <> 0 THEN
            RETURN jsonb_build_object(
                'protocol','forest.domain-result/1','ok',false,
                'error',jsonb_build_object(
                    'code','AGENT_INPUT_ACQUISITION_CONFLICT',
                    'message','Memory qualification differs from empty selection','status',409
                )
            );
        END IF;
        RETURN jsonb_build_object(
            'protocol','forest.domain-result/1','ok',true,
            'value',jsonb_build_object(
                'protocol','forest.agent-input-acquisition/1',
                'status',selection_status,'run_id',requested_run,
                'input_batch_ref',NULL,'input_revision',NULL,
                'selected_input_end_seq',(selection->>'selected_input_end_seq')::bigint,
                'context_revision',NULL,'context_view_revision',NULL,
                'memory_qualified_count',0,'memory_pending_count',0
            )
        );
    END IF;
    IF selection->>'boundary' IS DISTINCT FROM 'MODEL_SAFE_POINT'
       OR selection->>'run_ref' IS DISTINCT FROM requested_run
       OR selection->>'input_batch_id' IS NULL
       OR selection->>'context_revision' IS NULL
       OR selection->>'context_view_revision' IS NULL
       OR jsonb_typeof(selection->'event_ids') IS DISTINCT FROM 'array'
       OR jsonb_typeof(selection->'trigger_claim') IS DISTINCT FROM 'object'
       OR memory_value->>'input_batch_ref' IS DISTINCT FROM selection->>'input_batch_id' THEN
        RETURN jsonb_build_object(
            'protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object(
                'code','AGENT_INPUT_ACQUISITION_CONFLICT',
                'message','Selected InputBatch fences do not agree','status',409
            )
        );
    END IF;
    event_count := jsonb_array_length(selection->'event_ids');
    IF qualified_count + pending_count <> event_count THEN
        RETURN jsonb_build_object(
            'protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object(
                'code','AGENT_INPUT_ACQUISITION_CONFLICT',
                'message','Memory qualification does not cover the InputBatch','status',409
            )
        );
    END IF;
    batch_value := __forest_module_schema__.agent_batch_from_selection(selection);
    append_result := __forest_module_schema__.agent_append_run_input(
        context,
        jsonb_build_object(
            'protocol','forest.uow-step/1',
            'caller',envelope->'caller',
            'input',jsonb_build_object('owner',owner_value,'input_batch',batch_value),
            'prior',jsonb_build_object()
        )
    );
    IF append_result->>'ok' IS DISTINCT FROM 'true' THEN
        RETURN append_result;
    END IF;
    snapshot := append_result->'value';
    RETURN jsonb_build_object(
        'protocol','forest.domain-result/1','ok',true,
        'value',jsonb_build_object(
            'protocol','forest.agent-input-acquisition/1',
            'status','BATCH','run_id',requested_run,
            'input_batch_ref',selection->>'input_batch_id',
            'input_revision',(snapshot#>>'{progress,input_revision}')::bigint,
            'selected_input_end_seq',(selection->>'selected_input_end_seq')::bigint,
            'context_revision',(selection->>'context_revision')::bigint,
            'context_view_revision',(selection->>'context_view_revision')::bigint,
            'memory_qualified_count',qualified_count,
            'memory_pending_count',pending_count
        )
    );
END;
$$;

CREATE FUNCTION agent_park_reload(context jsonb, envelope jsonb)
RETURNS jsonb
LANGUAGE plpgsql
VOLATILE
SET search_path = pg_catalog
AS $$
DECLARE
    body jsonb := envelope->'input';
    owner_value jsonb := body->'owner';
    state_value jsonb := body->'state';
    requested_bot text := context->>'bot_id';
    requested_digest text := context->>'participant_input_digest';
    requested_run text := owner_value->>'run_id';
    requested_owner text := owner_value->>'owner_id';
    requested_epoch bigint := (owner_value->>'epoch')::bigint;
    requested_checkpoint text := body->>'checkpoint_id';
    requested_reload text := body->>'reload_operation_id';
    requested_resume text := body->>'resume_phase';
    run_row record;
    existing record;
    next_sequence bigint;
    checksum_value text;
BEGIN
    IF envelope->>'protocol' IS DISTINCT FROM 'forest.uow-step/1'
       OR envelope#>>'{caller,component_id}' IS DISTINCT FROM 'agent.runtime'
       OR requested_digest !~ '^sha256:[a-f0-9]{64}$'
       OR state_value->>'owner_component_id' IS DISTINCT FROM 'agent.runtime'
       OR state_value->>'owner_kind' IS DISTINCT FROM 'checkpoint.state'
       OR requested_resume NOT IN (
           'PREPARING_MODEL','REPAIR_READY','WAIT_TOOL','WAIT_APPROVAL','SAFE_BOUNDARY'
       ) THEN
        RETURN jsonb_build_object(
            'protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object(
                'code','AGENT_RELOAD_BOUNDARY_INVALID',
                'message','Agent reload checkpoint is invalid','status',422
            )
        );
    END IF;
    PERFORM pg_advisory_xact_lock(hashtextextended(
        'forest.agent.runtime/checkpoint/' || requested_bot || '/' || requested_checkpoint,0
    ));
    SELECT * INTO existing
    FROM __forest_module_schema__.run_checkpoints
    WHERE bot_id=requested_bot AND checkpoint_id=requested_checkpoint;
    IF FOUND THEN
        SELECT *,clock_timestamp() AS database_now INTO run_row
        FROM __forest_module_schema__.agent_runs
        WHERE bot_id=requested_bot AND run_id=requested_run FOR UPDATE;
        IF NOT FOUND OR run_row.owner_id IS DISTINCT FROM requested_owner
           OR run_row.owner_epoch IS DISTINCT FROM requested_epoch
           OR run_row.owner_expires_at IS NULL
           OR run_row.owner_expires_at<=run_row.database_now
           OR existing.run_id IS DISTINCT FROM requested_run
           OR existing.input_digest IS DISTINCT FROM requested_digest
           OR existing.phase IS DISTINCT FROM 'RELOAD_WAIT'
           OR run_row.reload_generation_id IS DISTINCT FROM context->>'generation_id' THEN
            RETURN jsonb_build_object(
                'protocol','forest.domain-result/1','ok',false,
                'error',jsonb_build_object(
                    'code','AGENT_CHECKPOINT_CONFLICT',
                    'message','Reload checkpoint identity differs','status',409
                )
            );
        END IF;
        RETURN __forest_module_schema__.agent_run_snapshot(
            context,jsonb_build_object('run_id',requested_run)
        );
    END IF;
    SELECT *,clock_timestamp() AS database_now INTO run_row
    FROM __forest_module_schema__.agent_runs
    WHERE bot_id=requested_bot AND run_id=requested_run FOR UPDATE;
    IF NOT FOUND OR run_row.owner_id IS DISTINCT FROM requested_owner
       OR run_row.owner_epoch IS DISTINCT FROM requested_epoch
       OR run_row.owner_expires_at IS NULL
       OR run_row.owner_expires_at<=run_row.database_now THEN
        RETURN jsonb_build_object(
            'protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object(
                'code','STALE_AGENT_RUN_OWNER','message','Agent Run owner is stale','status',409
            )
        );
    END IF;
    IF run_row.state IS DISTINCT FROM requested_resume THEN
        RETURN jsonb_build_object(
            'protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object(
                'code','AGENT_RELOAD_BOUNDARY_INVALID',
                'message','Agent Run is not at the declared reload boundary','status',409
            )
        );
    END IF;
    next_sequence := run_row.checkpoint_seq+1;
    checksum_value := 'sha256:' || encode(sha256(convert_to(jsonb_build_array(
        requested_run,next_sequence,'RELOAD_WAIT',run_row.input_revision,
        run_row.next_step_seq,state_value->>'digest',requested_epoch
    )::text,'UTF8')),'hex');
    INSERT INTO __forest_module_schema__.run_checkpoints(
        bot_id,run_id,checkpoint_seq,checkpoint_id,input_digest,phase,
        input_revision,next_step_seq,state_payload_id,state_payload_digest,
        state_owner_component_id,state_owner_kind,state_owner_id,
        checkpoint_checksum,owner_epoch
    ) VALUES (
        requested_bot,requested_run,next_sequence,requested_checkpoint,requested_digest,
        'RELOAD_WAIT',run_row.input_revision,run_row.next_step_seq,
        state_value->>'payload_id',state_value->>'digest',
        state_value->>'owner_component_id',state_value->>'owner_kind',state_value->>'owner_id',
        checksum_value,requested_epoch
    );
    UPDATE __forest_module_schema__.agent_runs
    SET state='RELOAD_WAIT',checkpoint_seq=next_sequence,
        reload_operation_id=requested_reload,reload_resume_phase=requested_resume,
        reload_generation_id=context->>'generation_id',
        updated_at=clock_timestamp()
    WHERE bot_id=requested_bot AND run_id=requested_run;
    RETURN __forest_module_schema__.agent_run_snapshot(
        context,jsonb_build_object('run_id',requested_run)
    );
END;
$$;

CREATE FUNCTION agent_resume_reload(context jsonb, envelope jsonb)
RETURNS jsonb
LANGUAGE plpgsql
VOLATILE
SET search_path = pg_catalog
AS $$
DECLARE
    body jsonb := envelope->'input';
    owner_value jsonb := body->'owner';
    requested_bot text := context->>'bot_id';
    requested_run text := owner_value->>'run_id';
    requested_owner text := owner_value->>'owner_id';
    requested_epoch bigint := (owner_value->>'epoch')::bigint;
    requested_reload text := body->>'reload_operation_id';
    requested_resume text := body->>'resume_phase';
    parked_generation text := body->>'parked_generation_id';
    resuming_generation text := context->>'generation_id';
    run_row record;
BEGIN
    IF envelope->>'protocol' IS DISTINCT FROM 'forest.uow-step/1'
       OR envelope#>>'{caller,component_id}' IS DISTINCT FROM 'agent.runtime'
       OR requested_resume NOT IN (
           'PREPARING_MODEL','REPAIR_READY','WAIT_TOOL','WAIT_APPROVAL','SAFE_BOUNDARY'
       )
       OR resuming_generation IS NULL
       OR (
           resuming_generation IS NOT DISTINCT FROM parked_generation
           AND context->>'recovered_generation' IS DISTINCT FROM 'true'
       ) THEN
        RETURN jsonb_build_object(
            'protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object(
                'code','AGENT_RELOAD_GENERATION_INVALID',
                'message','Only a replacement Agent Generation may resume parked work',
                'status',409
            )
        );
    END IF;
    SELECT *,clock_timestamp() AS database_now INTO run_row
    FROM __forest_module_schema__.agent_runs
    WHERE bot_id=requested_bot AND run_id=requested_run FOR UPDATE;
    IF NOT FOUND OR run_row.owner_id IS DISTINCT FROM requested_owner
       OR run_row.owner_epoch IS DISTINCT FROM requested_epoch
       OR run_row.owner_expires_at IS NULL
       OR run_row.owner_expires_at<=run_row.database_now THEN
        RETURN jsonb_build_object(
            'protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object(
                'code','STALE_AGENT_RUN_OWNER','message','Agent Run owner is stale','status',409
            )
        );
    END IF;
    IF run_row.state IS DISTINCT FROM 'RELOAD_WAIT'
       OR run_row.reload_operation_id IS DISTINCT FROM requested_reload
       OR run_row.reload_resume_phase IS DISTINCT FROM requested_resume
       OR run_row.reload_generation_id IS DISTINCT FROM parked_generation THEN
        RETURN jsonb_build_object(
            'protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object(
                'code','AGENT_RELOAD_CLAIM_CONFLICT',
                'message','Parked Agent Run identity differs from the claimed work',
                'status',409
            )
        );
    END IF;
    UPDATE __forest_module_schema__.agent_runs
    SET state=requested_resume,reload_operation_id=NULL,reload_resume_phase=NULL,
        reload_generation_id=NULL,updated_at=clock_timestamp()
    WHERE bot_id=requested_bot AND run_id=requested_run;
    RETURN jsonb_build_object(
        'protocol','forest.domain-result/1','ok',true,
        'value',jsonb_build_object(
            'run_id',requested_run,'reload_operation_id',requested_reload,
            'resumed_phase',requested_resume,'parked_generation_id',parked_generation,
            'resuming_generation_id',resuming_generation
        )
    );
END;
$$;

CREATE FUNCTION agent_claim_reload_resume(context jsonb, input jsonb)
RETURNS jsonb
LANGUAGE plpgsql
VOLATILE
SET search_path = pg_catalog
AS $$
DECLARE
    requested_bot text := context->>'bot_id';
    requested_generation text := context->>'generation_id';
    requested_token text := input->>'claim_token';
    requested_lease integer := (input->>'lease_seconds')::integer;
    run_row record;
    next_epoch bigint;
    identity_digest text;
BEGIN
    IF input->>'protocol' IS DISTINCT FROM 'forest.owned-work-claim-request/1'
       OR requested_generation IS NULL
       OR requested_token IS NULL
       OR octet_length(requested_token) NOT BETWEEN 1 AND 256
       OR requested_lease IS NULL OR requested_lease NOT BETWEEN 3 AND 3600 THEN
        RETURN jsonb_build_object(
            'protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object(
                'code','AGENT_RELOAD_CLAIM_INPUT_INVALID',
                'message','Agent reload claim is invalid','status',422
            )
        );
    END IF;
    SELECT * INTO run_row
    FROM __forest_module_schema__.agent_runs
    WHERE bot_id=requested_bot AND state='RELOAD_WAIT'
      AND (
          reload_generation_id IS DISTINCT FROM requested_generation
          OR context->>'recovered_generation'='true'
      )
      AND owner_id=requested_token AND owner_expires_at>clock_timestamp()
    FOR UPDATE;
    IF run_row IS NULL THEN
        SELECT candidate.* INTO run_row
        FROM __forest_module_schema__.agent_runs candidate
        WHERE candidate.bot_id=requested_bot AND candidate.state='RELOAD_WAIT'
          AND (
              candidate.reload_generation_id IS DISTINCT FROM requested_generation
              OR context->>'recovered_generation'='true'
          )
          AND (candidate.owner_id IS NULL OR candidate.owner_expires_at<=clock_timestamp())
        ORDER BY candidate.updated_at,candidate.run_id
        FOR UPDATE SKIP LOCKED
        LIMIT 1;
        IF run_row IS NULL THEN
            RETURN jsonb_build_object(
                'protocol','forest.domain-result/1','ok',true,'value',NULL
            );
        END IF;
        next_epoch := run_row.owner_epoch+1;
        UPDATE __forest_module_schema__.agent_runs
        SET owner_id=requested_token,owner_epoch=next_epoch,
            owner_expires_at=clock_timestamp()+(requested_lease*interval '1 second'),
            updated_at=clock_timestamp()
        WHERE bot_id=requested_bot AND run_id=run_row.run_id
        RETURNING * INTO run_row;
    END IF;
    identity_digest := encode(sha256(convert_to(jsonb_build_array(
        requested_bot,run_row.run_id,run_row.owner_epoch,run_row.reload_operation_id
    )::text,'UTF8')),'hex');
    RETURN jsonb_build_object(
        'protocol','forest.domain-result/1','ok',true,
        'value',jsonb_build_object(
            'protocol','forest.owned-work-claim/1',
            'work_id',run_row.run_id,
            'claim_token',requested_token,
            'attempt',run_row.owner_epoch,
            'operation_id','agent-reload-resume:' || identity_digest,
            'causal_root_kind','agent.reload-resume',
            'causal_root_id',run_row.run_id,
            'payload',jsonb_build_object(
                'owner',jsonb_build_object(
                    'run_id',run_row.run_id,'owner_id',requested_token,
                    'epoch',run_row.owner_epoch
                ),
                'reload_operation_id',run_row.reload_operation_id,
                'resume_phase',run_row.reload_resume_phase,
                'parked_generation_id',run_row.reload_generation_id
            )
        )
    );
END;
$$;

CREATE FUNCTION agent_settle_reload_resume(context jsonb, input jsonb)
RETURNS jsonb
LANGUAGE plpgsql
VOLATILE
SET search_path = pg_catalog
AS $$
DECLARE
    requested_bot text := context->>'bot_id';
    requested_work text := input->>'work_id';
    requested_token text := input->>'claim_token';
    requested_attempt bigint := (input->>'attempt')::bigint;
    existing record;
    run_row record;
    result_value jsonb;
BEGIN
    IF input->>'protocol' IS DISTINCT FROM 'forest.owned-work-settle-request/1'
       OR requested_work IS NULL OR requested_token IS NULL
       OR requested_attempt IS NULL OR requested_attempt<1
       OR input#>>'{outcome,status}' NOT IN ('SUCCEEDED','FAILED') THEN
        RETURN jsonb_build_object(
            'protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object(
                'code','AGENT_RELOAD_SETTLEMENT_INPUT_INVALID',
                'message','Agent reload settlement is invalid','status',422
            )
        );
    END IF;
    SELECT * INTO existing
    FROM __forest_module_schema__.agent_reload_resume_settlements
    WHERE claim_token=requested_token;
    IF existing IS NOT NULL THEN
        IF existing.input_payload=input THEN
            RETURN existing.result_payload;
        END IF;
        RETURN jsonb_build_object(
            'protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object(
                'code','AGENT_RELOAD_SETTLEMENT_CONFLICT',
                'message','Agent reload claim was settled with another outcome','status',409
            )
        );
    END IF;
    SELECT * INTO run_row
    FROM __forest_module_schema__.agent_runs
    WHERE bot_id=requested_bot AND run_id=requested_work FOR UPDATE;
    IF run_row IS NULL
       OR run_row.owner_id IS DISTINCT FROM requested_token
       OR run_row.owner_epoch IS DISTINCT FROM requested_attempt THEN
        RETURN jsonb_build_object(
            'protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object(
                'code','STALE_AGENT_RUN_OWNER','message','Agent Run owner is stale','status',409
            )
        );
    END IF;
    UPDATE __forest_module_schema__.agent_runs
    SET owner_id=NULL,owner_expires_at=NULL,updated_at=clock_timestamp()
    WHERE bot_id=requested_bot AND run_id=requested_work;
    result_value := jsonb_build_object(
        'protocol','forest.domain-result/1','ok',true,
        'value',jsonb_build_object(
            'protocol','forest.owned-work-settled/1','work_id',requested_work
        )
    );
    INSERT INTO __forest_module_schema__.agent_reload_resume_settlements(
        claim_token,input_payload,result_payload
    ) VALUES (requested_token,input,result_value);
    RETURN result_value;
END;
$$;

CREATE FUNCTION agent_complete_run(context jsonb, envelope jsonb)
RETURNS jsonb
LANGUAGE plpgsql
VOLATILE
SET search_path = pg_catalog
AS $$
DECLARE
    body jsonb := envelope->'input';
    owner_value jsonb := body->'owner';
    state_value jsonb := body->'state';
    requested_bot text := context->>'bot_id';
    requested_digest text := context->>'participant_input_digest';
    requested_run text := owner_value->>'run_id';
    requested_owner text := owner_value->>'owner_id';
    requested_epoch bigint := (owner_value->>'epoch')::bigint;
    requested_checkpoint text := body->>'checkpoint_id';
    run_row record;
    existing record;
    next_sequence bigint;
    checksum_value text;
BEGIN
    IF envelope->>'protocol' IS DISTINCT FROM 'forest.uow-step/1'
       OR envelope#>>'{caller,component_id}' IS DISTINCT FROM 'conversation.scheduler'
       OR requested_digest !~ '^sha256:[a-f0-9]{64}$'
       OR state_value->>'owner_component_id' IS DISTINCT FROM 'agent.runtime'
       OR state_value->>'owner_kind' IS DISTINCT FROM 'checkpoint.state' THEN
        RETURN jsonb_build_object(
            'protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object(
                'code','AGENT_COMPLETION_INPUT_INVALID',
                'message','Agent completion envelope is invalid','status',422
            )
        );
    END IF;
    PERFORM pg_advisory_xact_lock(hashtextextended(
        'forest.agent.runtime/checkpoint/' || requested_bot || '/' || requested_checkpoint,0
    ));
    SELECT * INTO existing
    FROM __forest_module_schema__.run_checkpoints
    WHERE bot_id=requested_bot AND checkpoint_id=requested_checkpoint;
    IF FOUND THEN
        IF existing.run_id IS DISTINCT FROM requested_run
           OR existing.input_digest IS DISTINCT FROM requested_digest
           OR existing.phase IS DISTINCT FROM 'COMPLETED' THEN
            RETURN jsonb_build_object(
                'protocol','forest.domain-result/1','ok',false,
                'error',jsonb_build_object(
                    'code','AGENT_CHECKPOINT_CONFLICT',
                    'message','Completion checkpoint identity differs','status',409
                )
            );
        END IF;
        RETURN __forest_module_schema__.agent_run_snapshot(
            context,jsonb_build_object('run_id',requested_run)
        );
    END IF;
    SELECT *,clock_timestamp() AS database_now INTO run_row
    FROM __forest_module_schema__.agent_runs
    WHERE bot_id=requested_bot AND run_id=requested_run FOR UPDATE;
    IF NOT FOUND OR run_row.owner_id IS DISTINCT FROM requested_owner
       OR run_row.owner_epoch IS DISTINCT FROM requested_epoch
       OR run_row.owner_expires_at IS NULL
       OR run_row.owner_expires_at<=run_row.database_now THEN
        RETURN jsonb_build_object(
            'protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object(
                'code','STALE_AGENT_RUN_OWNER','message','Agent Run owner is stale','status',409
            )
        );
    END IF;
    IF run_row.state IS DISTINCT FROM 'FINAL_READY' THEN
        RETURN jsonb_build_object(
            'protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object(
                'code','AGENT_COMPLETION_CONFLICT',
                'message','Agent Run has no final candidate ready to commit','status',409
            )
        );
    END IF;
    next_sequence := run_row.checkpoint_seq+1;
    checksum_value := 'sha256:' || encode(sha256(convert_to(jsonb_build_array(
        requested_run,next_sequence,'COMPLETED',run_row.input_revision,
        run_row.next_step_seq,state_value->>'digest',requested_epoch
    )::text,'UTF8')),'hex');
    INSERT INTO __forest_module_schema__.run_checkpoints(
        bot_id,run_id,checkpoint_seq,checkpoint_id,input_digest,phase,
        input_revision,next_step_seq,state_payload_id,state_payload_digest,
        state_owner_component_id,state_owner_kind,state_owner_id,
        checkpoint_checksum,owner_epoch
    ) VALUES (
        requested_bot,requested_run,next_sequence,requested_checkpoint,requested_digest,
        'COMPLETED',run_row.input_revision,run_row.next_step_seq,
        state_value->>'payload_id',state_value->>'digest',
        state_value->>'owner_component_id',state_value->>'owner_kind',state_value->>'owner_id',
        checksum_value,requested_epoch
    );
    UPDATE __forest_module_schema__.agent_runs
    SET state='COMPLETED',checkpoint_seq=next_sequence,terminal_at=run_row.database_now,
        updated_at=clock_timestamp()
    WHERE bot_id=requested_bot AND run_id=requested_run;
    RETURN __forest_module_schema__.agent_run_snapshot(
        context,jsonb_build_object('run_id',requested_run)
    );
END;
$$;

CREATE FUNCTION agent_start_successor(context jsonb, envelope jsonb)
RETURNS jsonb
LANGUAGE plpgsql
VOLATILE
SET search_path = pg_catalog
AS $$
DECLARE
    body jsonb := envelope->'input';
    batch jsonb := body->'input_batch';
    requested_bot text := context->>'bot_id';
    requested_operation text := context->>'operation_id';
    requested_digest text := context->>'participant_input_digest';
    requested_session text := body->>'session_id';
    requested_completed text := body->>'completed_run_id';
    requested_run text := body->>'run_id';
    deadline_value timestamptz;
    session_row record;
    previous record;
    existing record;
BEGIN
    IF envelope->>'protocol' IS DISTINCT FROM 'forest.uow-step/1'
       OR envelope#>>'{caller,component_id}' IS DISTINCT FROM 'conversation.scheduler'
       OR requested_operation IS NULL
       OR requested_digest !~ '^sha256:[a-f0-9]{64}$' THEN
        RETURN jsonb_build_object(
            'protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object(
                'code','AGENT_SUCCESSOR_INPUT_INVALID',
                'message','Agent successor envelope is invalid','status',422
            )
        );
    END IF;
    BEGIN
        deadline_value := (body->>'absolute_deadline')::timestamptz;
    EXCEPTION WHEN invalid_datetime_format OR datetime_field_overflow THEN
        RETURN jsonb_build_object(
            'protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object(
                'code','AGENT_SUCCESSOR_INPUT_INVALID',
                'message','Agent successor deadline is invalid','status',422
            )
        );
    END;
    IF NOT __forest_module_schema__.agent_claim_input_valid(batch) THEN
        RETURN jsonb_build_object(
            'protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object(
                'code','AGENT_SUCCESSOR_INPUT_INVALID',
                'message','Agent successor trigger claim is invalid','status',422
            )
        );
    END IF;
    PERFORM pg_advisory_xact_lock(hashtextextended(
        'forest.agent.runtime/start/' || requested_bot || '/' || requested_operation,0
    ));
    SELECT * INTO existing
    FROM __forest_module_schema__.agent_runs
    WHERE bot_id=requested_bot
      AND (start_operation_id=requested_operation OR run_id=requested_run)
    FOR UPDATE;
    IF FOUND THEN
        IF existing.start_operation_id IS DISTINCT FROM requested_operation
           OR existing.input_digest IS DISTINCT FROM requested_digest THEN
            RETURN jsonb_build_object(
                'protocol','forest.domain-result/1','ok',false,
                'error',jsonb_build_object(
                    'code','AGENT_START_OPERATION_CONFLICT',
                    'message','Agent successor start input differs','status',409
                )
            );
        END IF;
        RETURN __forest_module_schema__.agent_run_snapshot(
            context,jsonb_build_object('run_id',requested_run)
        );
    END IF;
    PERFORM pg_advisory_xact_lock(hashtextextended(
        'forest.agent.runtime/session/' || requested_bot || '/' || requested_session,0
    ));
    SELECT * INTO session_row
    FROM __forest_module_schema__.agent_sessions
    WHERE bot_id=requested_bot AND session_id=requested_session FOR UPDATE;
    SELECT *,clock_timestamp() AS database_now INTO previous
    FROM __forest_module_schema__.agent_runs
    WHERE bot_id=requested_bot AND run_id=requested_completed FOR UPDATE;
    IF previous IS NULL OR deadline_value<=previous.database_now
       OR deadline_value>previous.database_now+interval '24 hours' THEN
        RETURN jsonb_build_object(
            'protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object(
                'code','AGENT_SUCCESSOR_INPUT_INVALID',
                'message','Agent successor deadline is outside its bound','status',422
            )
        );
    END IF;
    IF session_row IS NULL OR session_row.state IS DISTINCT FROM 'ACTIVE'
       OR session_row.active_run_id IS DISTINCT FROM requested_completed
       OR previous.session_id IS DISTINCT FROM requested_session
       OR previous.state IS DISTINCT FROM 'COMPLETED' THEN
        RETURN jsonb_build_object(
            'protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object(
                'code','AGENT_SUCCESSOR_CONFLICT',
                'message','Session is not ready for the requested successor Run','status',409
            )
        );
    END IF;
    INSERT INTO __forest_module_schema__.agent_runs(
        bot_id,run_id,session_id,start_operation_id,input_digest,state,
        agent_spec_revision,config_payload_id,config_payload_digest,
        config_owner_component_id,config_owner_kind,config_owner_id,
        preset_revision,preset_payload_id,preset_payload_digest,
        preset_owner_component_id,preset_owner_kind,preset_owner_id,
        core_protocol_id,core_protocol_version,core_prompt_digest,parser_revision,
        absolute_deadline,input_revision,selected_input_end_seq,
        context_revision,context_view_revision
    ) VALUES (
        requested_bot,requested_run,requested_session,requested_operation,requested_digest,
        'PREPARING_MODEL',previous.agent_spec_revision,previous.config_payload_id,
        previous.config_payload_digest,previous.config_owner_component_id,
        previous.config_owner_kind,previous.config_owner_id,previous.preset_revision,
        previous.preset_payload_id,previous.preset_payload_digest,
        previous.preset_owner_component_id,previous.preset_owner_kind,previous.preset_owner_id,
        previous.core_protocol_id,previous.core_protocol_version,previous.core_prompt_digest,
        previous.parser_revision,deadline_value,1,
        (batch->>'selected_input_end_seq')::bigint,(batch->>'context_revision')::bigint,
        (batch->>'context_view_revision')::bigint
    );
    INSERT INTO __forest_module_schema__.run_inputs(
        bot_id,run_id,input_revision,input_batch_ref,input_batch_digest,
        selected_input_end_seq,context_revision,context_view_revision,
        source_manifest_digest,trigger_claim_ref,trigger_claim_digest,
        trigger_count,last_trigger_ref,last_trigger_ingress_seq
    ) VALUES (
        requested_bot,requested_run,1,batch->>'input_batch_ref',batch->>'input_batch_digest',
        (batch->>'selected_input_end_seq')::bigint,(batch->>'context_revision')::bigint,
        (batch->>'context_view_revision')::bigint,batch->>'source_manifest_digest',
        batch#>>'{trigger_claim,ref}',batch#>>'{trigger_claim,digest}',
        (batch#>>'{trigger_claim,count}')::integer,
        batch#>>'{trigger_claim,last_trigger_ref}',
        (batch#>>'{trigger_claim,last_trigger_ingress_seq}')::bigint
    );
    UPDATE __forest_module_schema__.agent_sessions
    SET active_run_id=requested_run,session_epoch=session_epoch+1,
        updated_at=clock_timestamp()
    WHERE bot_id=requested_bot AND session_id=requested_session;
    RETURN __forest_module_schema__.agent_run_snapshot(
        context,jsonb_build_object('run_id',requested_run)
    );
END;
$$;

CREATE FUNCTION agent_close_session(context jsonb, envelope jsonb)
RETURNS jsonb
LANGUAGE plpgsql
VOLATILE
SET search_path = pg_catalog
AS $$
DECLARE
    body jsonb := envelope->'input';
    requested_bot text := context->>'bot_id';
    requested_operation text := context->>'operation_id';
    requested_digest text := context->>'participant_input_digest';
    requested_session text := body->>'session_id';
    requested_completed text := body->>'completed_run_id';
    session_row record;
    run_row record;
    existing record;
    closed_value timestamptz;
BEGIN
    IF envelope->>'protocol' IS DISTINCT FROM 'forest.uow-step/1'
       OR envelope#>>'{caller,component_id}' IS DISTINCT FROM 'conversation.scheduler'
       OR requested_operation IS NULL
       OR requested_digest !~ '^sha256:[a-f0-9]{64}$' THEN
        RETURN jsonb_build_object(
            'protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object(
                'code','AGENT_SESSION_CLOSE_INPUT_INVALID',
                'message','Agent Session close envelope is invalid','status',422
            )
        );
    END IF;
    PERFORM pg_advisory_xact_lock(hashtextextended(
        'forest.agent.runtime/session-close/' || requested_bot || '/' || requested_operation,0
    ));
    SELECT * INTO existing
    FROM __forest_module_schema__.session_close_operations
    WHERE bot_id=requested_bot AND operation_id=requested_operation;
    IF FOUND THEN
        IF existing.input_digest IS DISTINCT FROM requested_digest THEN
            RETURN jsonb_build_object(
                'protocol','forest.domain-result/1','ok',false,
                'error',jsonb_build_object(
                    'code','AGENT_SESSION_CLOSE_CONFLICT',
                    'message','Agent Session close input differs','status',409
                )
            );
        END IF;
        RETURN jsonb_build_object(
            'protocol','forest.domain-result/1','ok',true,
            'value',jsonb_build_object(
                'operation_id',existing.operation_id,'session_id',existing.session_id,
                'completed_run_id',existing.completed_run_id,'closed_at',existing.closed_at
            )
        );
    END IF;
    PERFORM pg_advisory_xact_lock(hashtextextended(
        'forest.agent.runtime/session/' || requested_bot || '/' || requested_session,0
    ));
    SELECT * INTO session_row
    FROM __forest_module_schema__.agent_sessions
    WHERE bot_id=requested_bot AND session_id=requested_session FOR UPDATE;
    SELECT * INTO run_row
    FROM __forest_module_schema__.agent_runs
    WHERE bot_id=requested_bot AND run_id=requested_completed FOR UPDATE;
    IF session_row IS NULL OR run_row IS NULL
       OR session_row.state IS DISTINCT FROM 'ACTIVE'
       OR session_row.active_run_id IS DISTINCT FROM requested_completed
       OR run_row.session_id IS DISTINCT FROM requested_session
       OR run_row.state IS DISTINCT FROM 'COMPLETED' THEN
        RETURN jsonb_build_object(
            'protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object(
                'code','AGENT_SESSION_CLOSE_CONFLICT',
                'message','Session is not idle at the completed Run','status',409
            )
        );
    END IF;
    closed_value := clock_timestamp();
    UPDATE __forest_module_schema__.agent_sessions
    SET state='CLOSED',active_run_id=NULL,closed_at=closed_value,
        updated_at=closed_value
    WHERE bot_id=requested_bot AND session_id=requested_session;
    INSERT INTO __forest_module_schema__.session_close_operations(
        bot_id,operation_id,input_digest,session_id,completed_run_id,closed_at
    ) VALUES (
        requested_bot,requested_operation,requested_digest,
        requested_session,requested_completed,closed_value
    );
    RETURN jsonb_build_object(
        'protocol','forest.domain-result/1','ok',true,
        'value',jsonb_build_object(
            'operation_id',requested_operation,'session_id',requested_session,
            'completed_run_id',requested_completed,'closed_at',closed_value
        )
    );
END;
$$;

CREATE FUNCTION agent_final_learning(context jsonb, input jsonb)
RETURNS jsonb LANGUAGE sql STABLE SET search_path = pg_catalog AS $$
    WITH pairs AS (
        SELECT r.step_seq,r.request_id,r.input_revision,i.last_trigger_ref AS trigger_ref,
            candidate.value AS seed,
            candidate.value->>'source_unit_id' AS seed_source_unit_id
        FROM __forest_module_schema__.model_requests r
        JOIN __forest_module_schema__.run_inputs i
          ON i.bot_id=r.bot_id AND i.run_id=r.run_id
         AND i.input_revision=r.input_revision
        CROSS JOIN LATERAL jsonb_array_elements(r.learning_candidates) candidate(value)
        WHERE r.bot_id=context->>'bot_id' AND r.run_id=input->>'run_id'
          AND r.state='SUCCEEDED' AND r.request_kind IN ('PRIMARY','CONTINUATION')
          AND EXISTS (
              SELECT 1 FROM __forest_module_schema__.model_turns turn_row
              WHERE turn_row.bot_id=r.bot_id AND turn_row.request_id=r.request_id
          )
    ), distinct_pairs AS (
        SELECT DISTINCT ON (trigger_ref,seed_source_unit_id) *
        FROM pairs
        ORDER BY trigger_ref,seed_source_unit_id,step_seq,request_id
    ), ordered AS (
        SELECT *,row_number() OVER (
            ORDER BY step_seq,trigger_ref,seed_source_unit_id,request_id
        ) AS position
        FROM distinct_pairs
        ORDER BY step_seq,trigger_ref,seed_source_unit_id,request_id
        LIMIT 129
    )
    SELECT jsonb_build_object(
        'protocol','forest.agent-learning/1',
        'proposals',COALESCE(jsonb_agg(jsonb_build_object(
            'request_id',request_id,'input_revision',input_revision,
            'trigger_ref',trigger_ref,'seed',seed
        ) ORDER BY position) FILTER (WHERE position<=128),'[]'::jsonb),
        'truncated',count(*)>128
    ) FROM ordered;
$$;

CREATE FUNCTION agent_commit_final_run(context jsonb, envelope jsonb)
RETURNS jsonb
LANGUAGE plpgsql
VOLATILE
SET search_path = pg_catalog
AS $$
DECLARE
    body jsonb := envelope->'input';
    reply_value jsonb := body->'reply';
    requested_bot text := context->>'bot_id';
    requested_digest text := context->>'participant_input_digest';
    requested_run text := body->>'run_id';
    requested_session text := body->>'session_id';
    requested_checkpoint text;
    run_row record;
    session_row record;
    existing record;
    next_sequence bigint;
    checksum_value text;
BEGIN
    IF envelope->>'protocol' IS DISTINCT FROM 'forest.uow-step/1'
       OR envelope#>>'{caller,component_id}' IS DISTINCT FROM 'conversation.scheduler'
       OR requested_digest !~ '^sha256:[a-f0-9]{64}$'
       OR reply_value->>'owner_component_id' IS DISTINCT FROM 'agent.runtime'
       OR reply_value->>'owner_kind' IS DISTINCT FROM 'final.reply'
       OR reply_value->>'digest' !~ '^sha256:[a-f0-9]{64}$' THEN
        RETURN jsonb_build_object(
            'protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object(
                'code','AGENT_FINAL_COMMIT_INPUT_INVALID',
                'message','Agent final commit envelope is invalid','status',422
            )
        );
    END IF;
    requested_checkpoint := 'checkpoint-completed:' || encode(sha256(convert_to(
        jsonb_build_array(requested_bot,requested_run)::text,'UTF8'
    )),'hex');
    PERFORM pg_advisory_xact_lock(hashtextextended(
        'forest.agent.runtime/final-commit/' || requested_bot || '/' || requested_run,0
    ));
    SELECT * INTO run_row
    FROM __forest_module_schema__.agent_runs
    WHERE bot_id=requested_bot AND run_id=requested_run FOR UPDATE;
    SELECT * INTO session_row
    FROM __forest_module_schema__.agent_sessions
    WHERE bot_id=requested_bot AND session_id=requested_session FOR UPDATE;
    IF run_row IS NULL OR session_row IS NULL
       OR run_row.session_id IS DISTINCT FROM requested_session
       OR session_row.state IS DISTINCT FROM 'ACTIVE'
       OR session_row.active_run_id IS DISTINCT FROM requested_run
       OR run_row.final_candidate_payload_id IS DISTINCT FROM reply_value->>'payload_id'
       OR run_row.final_candidate_payload_digest IS DISTINCT FROM reply_value->>'digest' THEN
        RETURN jsonb_build_object(
            'protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object(
                'code','AGENT_FINAL_COMMIT_CONFLICT',
                'message','Agent final candidate differs from the active Run','status',409
            )
        );
    END IF;
    SELECT * INTO existing
    FROM __forest_module_schema__.run_checkpoints
    WHERE bot_id=requested_bot AND checkpoint_id=requested_checkpoint;
    IF existing IS NOT NULL THEN
        IF existing.run_id IS DISTINCT FROM requested_run
           OR existing.input_digest IS DISTINCT FROM requested_digest
           OR existing.phase IS DISTINCT FROM 'COMPLETED'
           OR run_row.state IS DISTINCT FROM 'COMPLETED' THEN
            RETURN jsonb_build_object(
                'protocol','forest.domain-result/1','ok',false,
                'error',jsonb_build_object(
                    'code','AGENT_FINAL_COMMIT_CONFLICT',
                    'message','Agent final commit replay differs','status',409
                )
            );
        END IF;
        RETURN jsonb_set(__forest_module_schema__.agent_run_snapshot(
            context,jsonb_build_object('run_id',requested_run)
        ),'{value,learning}',__forest_module_schema__.agent_final_learning(
            context,jsonb_build_object('run_id',requested_run)
        ));
    END IF;
    IF run_row.state IS DISTINCT FROM 'FINAL_READY' OR run_row.owner_id IS NOT NULL THEN
        RETURN jsonb_build_object(
            'protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object(
                'code','AGENT_FINAL_COMMIT_CONFLICT',
                'message','Agent Run is not at an unowned final boundary','status',409
            )
        );
    END IF;
    next_sequence := run_row.checkpoint_seq+1;
    checksum_value := 'sha256:' || encode(sha256(convert_to(jsonb_build_array(
        requested_run,next_sequence,'COMPLETED',run_row.input_revision,
        run_row.next_step_seq,reply_value->>'digest',run_row.owner_epoch
    )::text,'UTF8')),'hex');
    INSERT INTO __forest_module_schema__.run_checkpoints(
        bot_id,run_id,checkpoint_seq,checkpoint_id,input_digest,phase,
        input_revision,next_step_seq,state_payload_id,state_payload_digest,
        state_owner_component_id,state_owner_kind,state_owner_id,
        checkpoint_checksum,owner_epoch
    ) VALUES (
        requested_bot,requested_run,next_sequence,requested_checkpoint,requested_digest,
        'COMPLETED',run_row.input_revision,run_row.next_step_seq,
        reply_value->>'payload_id',reply_value->>'digest',
        reply_value->>'owner_component_id',reply_value->>'owner_kind',
        reply_value->>'owner_id',checksum_value,GREATEST(run_row.owner_epoch,1)
    );
    UPDATE __forest_module_schema__.agent_runs
    SET state='COMPLETED',checkpoint_seq=next_sequence,
        terminal_at=clock_timestamp(),updated_at=clock_timestamp()
    WHERE bot_id=requested_bot AND run_id=requested_run;
    RETURN jsonb_set(__forest_module_schema__.agent_run_snapshot(
        context,jsonb_build_object('run_id',requested_run)
    ),'{value,learning}',__forest_module_schema__.agent_final_learning(
        context,jsonb_build_object('run_id',requested_run)
    ));
END;
$$;

CREATE FUNCTION agent_handoff_session(context jsonb, envelope jsonb)
RETURNS jsonb
LANGUAGE plpgsql
VOLATILE
SET search_path = pg_catalog
AS $$
DECLARE
    body jsonb := envelope->'input';
    selection jsonb := envelope#>'{prior,conversation}';
    memory_value jsonb := envelope#>'{prior,memory}';
    requested_session text := body->>'session_id';
    requested_completed text := body->>'completed_run_id';
    requested_successor text := body->>'successor_run_id';
    batch_value jsonb;
    delegated jsonb;
    event_count integer;
    qualified_count integer;
    pending_count integer;
BEGIN
    IF envelope->>'protocol' IS DISTINCT FROM 'forest.uow-step/1'
       OR envelope#>>'{caller,component_id}' IS DISTINCT FROM 'conversation.scheduler'
       OR body->>'selection_step_id' IS DISTINCT FROM 'conversation'
       OR body->>'memory_step_id' IS DISTINCT FROM 'memory'
       OR selection IS NULL OR memory_value IS NULL THEN
        RETURN jsonb_build_object(
            'protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object(
                'code','AGENT_SESSION_HANDOFF_INPUT_INVALID',
                'message','Agent Session handoff envelope is invalid','status',422
            )
        );
    END IF;
    IF selection->>'status'='WAIT_INPUT' THEN
        RETURN jsonb_build_object(
            'protocol','forest.domain-result/1','ok',true,
            'value',jsonb_build_object(
                'protocol','forest.agent-session-handoff/1','action','WAIT_INPUT',
                'selection',selection,'run',NULL,'session',NULL
            )
        );
    END IF;
    IF selection->>'status'='BATCH' AND selection->>'kind'='TRIGGER_INPUT' THEN
        event_count := jsonb_array_length(selection->'event_ids');
        qualified_count := (memory_value->>'qualified_count')::integer;
        pending_count := (memory_value->>'pending_count')::integer;
        IF memory_value->>'input_batch_ref' IS DISTINCT FROM selection->>'input_batch_id'
           OR qualified_count IS NULL OR pending_count IS NULL
           OR qualified_count+pending_count<>event_count THEN
            RETURN jsonb_build_object(
                'protocol','forest.domain-result/1','ok',false,
                'error',jsonb_build_object(
                    'code','AGENT_SESSION_HANDOFF_MEMORY_CONFLICT',
                    'message','Memory qualification does not cover successor input','status',409
                )
            );
        END IF;
        batch_value := __forest_module_schema__.agent_batch_from_selection(selection);
        delegated := __forest_module_schema__.agent_start_successor(
            context,jsonb_build_object(
                'protocol','forest.uow-step/1','caller',envelope->'caller',
                'input',jsonb_build_object(
                    'session_id',requested_session,'completed_run_id',requested_completed,
                    'run_id',requested_successor,'input_batch',batch_value,
                    'absolute_deadline',body->>'successor_deadline'
                ),'prior',jsonb_build_object()
            )
        );
        IF delegated->>'ok' IS DISTINCT FROM 'true' THEN
            RETURN delegated;
        END IF;
        RETURN jsonb_build_object(
            'protocol','forest.domain-result/1','ok',true,
            'value',jsonb_build_object(
                'protocol','forest.agent-session-handoff/1','action','SUCCESSOR',
                'selection',selection,'run',delegated->'value','session',NULL
            )
        );
    END IF;
    IF selection->>'status'='IDLE'
       OR (selection->>'status'='BATCH' AND selection->>'kind'='SILENT_DRAIN') THEN
        delegated := __forest_module_schema__.agent_close_session(
            context,jsonb_build_object(
                'protocol','forest.uow-step/1','caller',envelope->'caller',
                'input',jsonb_build_object(
                    'session_id',requested_session,'completed_run_id',requested_completed
                ),'prior',jsonb_build_object()
            )
        );
        IF delegated->>'ok' IS DISTINCT FROM 'true' THEN
            RETURN delegated;
        END IF;
        RETURN jsonb_build_object(
            'protocol','forest.domain-result/1','ok',true,
            'value',jsonb_build_object(
                'protocol','forest.agent-session-handoff/1','action','CLOSED',
                'selection',selection,'run',NULL,'session',delegated->'value'
            )
        );
    END IF;
    RETURN jsonb_build_object(
        'protocol','forest.domain-result/1','ok',false,
        'error',jsonb_build_object(
            'code','AGENT_SESSION_HANDOFF_CONFLICT',
            'message','Conversation returned an invalid Session boundary','status',409
        )
    );
END;
$$;
CREATE FUNCTION agent_verify_requeue(context jsonb, envelope jsonb)
RETURNS jsonb LANGUAGE plpgsql VOLATILE SET search_path=pg_catalog AS $$
DECLARE
    body jsonb := envelope->'input';
    prior_value jsonb := envelope->'prior'->(body->>'prior_step_id');
    run_row record;
BEGIN
    IF envelope->>'protocol' IS DISTINCT FROM 'forest.uow-step/1'
       OR envelope#>>'{caller,component_id}' IS DISTINCT FROM 'conversation.scheduler'
       OR body->>'protocol' IS DISTINCT FROM 'forest.agent-requeue-verification/1'
       OR body->>'prior_step_id' IS DISTINCT FROM 'conversation'
       OR prior_value->>'protocol' IS DISTINCT FROM 'forest.conversation-run-requeued/1'
       OR prior_value->>'session_ref' IS NULL
       OR prior_value->>'run_ref' IS NULL THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object('code','AGENT_REQUEUE_VERIFICATION_INVALID',
                'message','Requeue verification input is invalid','status',422));
    END IF;
    SELECT r.session_id,r.state,r.terminal_at,s.conversation_ref,
           s.state AS session_state,s.active_run_id,s.closed_at INTO run_row
    FROM __forest_module_schema__.agent_runs r
    JOIN __forest_module_schema__.agent_sessions s
      ON s.bot_id=r.bot_id AND s.session_id=r.session_id
    WHERE r.bot_id=context->>'bot_id'
      AND r.run_id=prior_value->>'run_ref'
    FOR SHARE;
    IF run_row IS NULL
       OR run_row.session_id IS DISTINCT FROM prior_value->>'session_ref'
       OR run_row.conversation_ref IS DISTINCT FROM prior_value->>'conversation_id'
       OR run_row.state NOT IN ('FAILED','CANCELLED')
       OR run_row.terminal_at IS NULL
       OR run_row.session_state NOT IN ('FAILED','CANCELLED')
       OR run_row.active_run_id IS NOT NULL
       OR run_row.closed_at IS NULL THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object('code','AGENT_RUN_NOT_ABANDONED',
                'message','Run is not terminal and eligible for requeue','status',409));
    END IF;
    IF EXISTS (
        SELECT 1 FROM __forest_module_schema__.tool_calls t
        WHERE t.bot_id=context->>'bot_id' AND t.run_id=prior_value->>'run_ref'
    ) OR EXISTS (
        SELECT 1 FROM __forest_module_schema__.model_requests r
        JOIN __forest_module_schema__.model_transport_attempts a
          ON a.bot_id=r.bot_id AND a.request_id=r.request_id
        WHERE r.bot_id=context->>'bot_id'
          AND r.run_id=prior_value->>'run_ref'
          AND a.state IN ('DISPATCHING','UNKNOWN')
    ) THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object('code','AGENT_REQUEUE_EFFECT_UNSAFE',
                'message','Run has a tool or unresolved model attempt','status',409));
    END IF;
    RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',true,
                              'value',prior_value);
END;
$$;

CREATE FUNCTION agent_replacement_guard(context jsonb, input jsonb)
RETURNS jsonb
LANGUAGE plpgsql
STABLE
SET search_path = pg_catalog
AS $$
DECLARE
    replaced text[];
    blockers integer;
BEGIN
    IF jsonb_typeof(input->'replaced_components') IS DISTINCT FROM 'array' THEN
        RETURN jsonb_build_object(
            'protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object(
                'code','AGENT_REPLACEMENT_GUARD_INPUT_INVALID',
                'message','Replacement component set is invalid','status',422
            )
        );
    END IF;
    SELECT array_agg(value) INTO replaced
    FROM jsonb_array_elements_text(input->'replaced_components') AS items(value);
    SELECT count(*) INTO blockers
    FROM __forest_module_schema__.model_requests request
    JOIN __forest_module_schema__.agent_runs run
      ON run.bot_id=request.bot_id AND run.run_id=request.run_id
    WHERE request.bot_id=context->>'bot_id'
      AND request.provider_component_id=ANY(replaced)
      AND request.state IN (
          'READY','ATTEMPT_PREPARED','DISPATCHING','NOT_APPLIED','UNKNOWN'
      )
      AND run.state NOT IN ('COMPLETED','FAILED','CANCELLED');
    RETURN jsonb_build_object(
        'protocol','forest.domain-result/1','ok',true,
        'value',jsonb_build_object(
            'protocol','forest.replacement-guard/1',
            'safe',blockers=0,'blocker_count',blockers
        )
    );
END;
$$;
