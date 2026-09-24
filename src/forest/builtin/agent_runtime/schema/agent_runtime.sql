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
        OR reload_resume_phase IN ('PREPARING_MODEL','WAIT_TOOL','WAIT_APPROVAL')
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
    trigger_count integer NOT NULL CHECK (trigger_count > 0),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (bot_id,run_id,input_revision),
    UNIQUE (bot_id,input_batch_ref),
    FOREIGN KEY (bot_id,run_id) REFERENCES agent_runs(bot_id,run_id)
);

CREATE TABLE run_input_triggers (
    bot_id text NOT NULL,
    run_id text NOT NULL,
    input_revision bigint NOT NULL,
    trigger_index integer NOT NULL CHECK (trigger_index >= 0),
    trigger_ref text NOT NULL CHECK (octet_length(trigger_ref) BETWEEN 1 AND 256),
    principal_ref text NOT NULL CHECK (octet_length(principal_ref) BETWEEN 1 AND 256),
    ingress_seq bigint NOT NULL CHECK (ingress_seq > 0),
    PRIMARY KEY (bot_id,run_id,input_revision,trigger_index),
    UNIQUE (bot_id,run_id,input_revision,trigger_ref),
    FOREIGN KEY (bot_id,run_id,input_revision)
        REFERENCES run_inputs(bot_id,run_id,input_revision)
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
    arguments_payload_id text NOT NULL
        CHECK (octet_length(arguments_payload_id) BETWEEN 1 AND 256),
    arguments_payload_digest text NOT NULL
        CHECK (arguments_payload_digest ~ '^sha256:[a-f0-9]{64}$'),
    authority_trigger_ref text NOT NULL
        CHECK (octet_length(authority_trigger_ref) BETWEEN 1 AND 256),
    authority_principal_ref text NOT NULL
        CHECK (octet_length(authority_principal_ref) BETWEEN 1 AND 256),
    tool_contract_digest text NOT NULL CHECK (tool_contract_digest ~ '^sha256:[a-f0-9]{64}$'),
    effect_operation_ref text NOT NULL
        CHECK (octet_length(effect_operation_ref) BETWEEN 1 AND 256),
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
       (OLD.state='REPAIR_READY' AND NEW.state IN ('PREPARING_MODEL','FAILED','CANCELLED')) OR
       (OLD.state='WAIT_TOOL' AND NEW.state IN ('PREPARING_MODEL','WAIT_APPROVAL','BLOCKED_UNKNOWN','FAILED','CANCELLED','RELOAD_WAIT')) OR
       (OLD.state='WAIT_APPROVAL' AND NEW.state IN ('PREPARING_MODEL','WAIT_TOOL','FAILED','CANCELLED','RELOAD_WAIT')) OR
       (OLD.state='BLOCKED_UNKNOWN' AND NEW.state IN ('MODEL_PENDING','PARSING','PREPARING_MODEL','WAIT_TOOL','FAILED','CANCELLED')) OR
       (OLD.state='SAFE_BOUNDARY' AND NEW.state IN ('PREPARING_MODEL','FINAL_READY','FAILED','CANCELLED')) OR
       (OLD.state='FINAL_READY' AND NEW.state IN ('COMPLETED','PREPARING_MODEL','FAILED','CANCELLED')) OR
       (OLD.state='RELOAD_WAIT' AND NEW.state IN ('PREPARING_MODEL','WAIT_TOOL','WAIT_APPROVAL','FAILED','CANCELLED')) THEN
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
CREATE TRIGGER run_input_triggers_immutable BEFORE UPDATE OR DELETE ON run_input_triggers
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
           NEW.source_manifest_digest,NEW.output_token_budget,NEW.created_at)
       IS DISTINCT FROM
       ROW(OLD.bot_id,OLD.request_id,OLD.run_id,OLD.step_seq,OLD.request_kind,
           OLD.semantic_request_digest,OLD.input_revision,OLD.context_revision,
           OLD.context_view_revision,OLD.core_protocol_version,OLD.core_prompt_digest,
           OLD.preset_revision,OLD.preset_payload_digest,OLD.provider_component_id,
           OLD.provider_generation_id,OLD.provider_profile_ref,OLD.model_ref,
           OLD.instruction_channel,OLD.capability_digest,OLD.projection_payload_id,
           OLD.projection_payload_digest,OLD.projection_owner_component_id,
           OLD.projection_owner_kind,OLD.projection_owner_id,OLD.projected_content_digest,
           OLD.source_manifest_digest,OLD.output_token_budget,OLD.created_at) THEN
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
           NEW.tool_name,NEW.arguments_payload_id,NEW.arguments_payload_digest,
           NEW.authority_trigger_ref,NEW.authority_principal_ref,
           NEW.tool_contract_digest,NEW.effect_operation_ref,NEW.created_at)
       IS DISTINCT FROM
       ROW(OLD.bot_id,OLD.tool_call_id,OLD.run_id,OLD.turn_id,OLD.step_seq,
           OLD.tool_name,OLD.arguments_payload_id,OLD.arguments_payload_digest,
           OLD.authority_trigger_ref,OLD.authority_principal_ref,
           OLD.tool_contract_digest,OLD.effect_operation_ref,OLD.created_at) THEN
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
    triggers_value jsonb;
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
        'source_manifest_digest',i.source_manifest_digest,'trigger_count',i.trigger_count,
        'created_at',i.created_at
    ) ORDER BY i.input_revision),'[]'::jsonb)
    INTO inputs_value
    FROM __forest_module_schema__.run_inputs i
    WHERE i.bot_id=requested_bot AND i.run_id=requested_run;
    SELECT COALESCE(jsonb_agg(jsonb_build_object(
        'input_revision',t.input_revision,'trigger_index',t.trigger_index,
        'trigger_ref',t.trigger_ref,'principal_ref',t.principal_ref,'ingress_seq',t.ingress_seq
    ) ORDER BY t.input_revision,t.trigger_index),'[]'::jsonb)
    INTO triggers_value
    FROM __forest_module_schema__.run_input_triggers t
    WHERE t.bot_id=requested_bot AND t.run_id=requested_run;
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
            'inputs',inputs_value,'triggers',triggers_value,'cancel_request',cancel_value,
            'created_at',run_row.created_at,'updated_at',run_row.updated_at,
            'terminal_at',run_row.terminal_at
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
    trigger_value jsonb;
    trigger_index integer := 0;
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
    IF EXISTS (
        SELECT 1 FROM jsonb_array_elements(batch->'triggers') item
        WHERE (item->>'ingress_seq')::bigint>(batch->>'selected_input_end_seq')::bigint
    ) OR (
        SELECT count(*)<>count(DISTINCT item->>'trigger_ref')
        FROM jsonb_array_elements(batch->'triggers') item
    ) THEN
        RETURN jsonb_build_object(
            'protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object(
                'code','AGENT_RUNTIME_INPUT_INVALID',
                'message','Agent input trigger set is invalid','status',422
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
        source_manifest_digest,trigger_count
    ) VALUES (
        requested_bot,requested_run,1,batch->>'input_batch_ref',batch->>'input_batch_digest',
        (batch->>'selected_input_end_seq')::bigint,(batch->>'context_revision')::bigint,
        (batch->>'context_view_revision')::bigint,batch->>'source_manifest_digest',
        jsonb_array_length(batch->'triggers')
    );
    FOR trigger_value IN SELECT value FROM jsonb_array_elements(batch->'triggers') LOOP
        INSERT INTO __forest_module_schema__.run_input_triggers(
            bot_id,run_id,input_revision,trigger_index,trigger_ref,principal_ref,ingress_seq
        ) VALUES (
            requested_bot,requested_run,1,trigger_index,trigger_value->>'trigger_ref',
            trigger_value->>'principal_ref',(trigger_value->>'ingress_seq')::bigint
        );
        trigger_index := trigger_index+1;
    END LOOP;
    RETURN __forest_module_schema__.agent_run_snapshot(
        context,jsonb_build_object('run_id',requested_run)
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
    trigger_value jsonb;
    trigger_index integer := 0;
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
           OR existing.trigger_count IS DISTINCT FROM jsonb_array_length(batch->'triggers') THEN
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
       OR EXISTS (
           SELECT 1 FROM jsonb_array_elements(batch->'triggers') item
           WHERE (item->>'ingress_seq')::bigint>(batch->>'selected_input_end_seq')::bigint
       )
       OR (
           SELECT count(*)<>count(DISTINCT item->>'trigger_ref')
           FROM jsonb_array_elements(batch->'triggers') item
       ) THEN
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
        source_manifest_digest,trigger_count
    ) VALUES (
        requested_bot,requested_run,next_revision,batch->>'input_batch_ref',
        batch->>'input_batch_digest',(batch->>'selected_input_end_seq')::bigint,
        (batch->>'context_revision')::bigint,(batch->>'context_view_revision')::bigint,
        batch->>'source_manifest_digest',jsonb_array_length(batch->'triggers')
    );
    FOR trigger_value IN SELECT value FROM jsonb_array_elements(batch->'triggers') LOOP
        INSERT INTO __forest_module_schema__.run_input_triggers(
            bot_id,run_id,input_revision,trigger_index,trigger_ref,principal_ref,ingress_seq
        ) VALUES (
            requested_bot,requested_run,next_revision,trigger_index,
            trigger_value->>'trigger_ref',trigger_value->>'principal_ref',
            (trigger_value->>'ingress_seq')::bigint
        );
        trigger_index := trigger_index+1;
    END LOOP;
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
       OR requested_resume NOT IN ('PREPARING_MODEL','WAIT_TOOL','WAIT_APPROVAL') THEN
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
       OR requested_resume NOT IN ('PREPARING_MODEL','WAIT_TOOL','WAIT_APPROVAL')
       OR resuming_generation IS NULL
       OR resuming_generation IS NOT DISTINCT FROM parked_generation THEN
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
      AND reload_generation_id IS DISTINCT FROM requested_generation
      AND owner_id=requested_token AND owner_expires_at>clock_timestamp()
    FOR UPDATE;
    IF run_row IS NULL THEN
        SELECT candidate.* INTO run_row
        FROM __forest_module_schema__.agent_runs candidate
        WHERE candidate.bot_id=requested_bot AND candidate.state='RELOAD_WAIT'
          AND candidate.reload_generation_id IS DISTINCT FROM requested_generation
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
    trigger_value jsonb;
    trigger_index integer := 0;
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
    IF EXISTS (
        SELECT 1 FROM jsonb_array_elements(batch->'triggers') item
        WHERE (item->>'ingress_seq')::bigint>(batch->>'selected_input_end_seq')::bigint
    ) OR (
        SELECT count(*)<>count(DISTINCT item->>'trigger_ref')
        FROM jsonb_array_elements(batch->'triggers') item
    ) THEN
        RETURN jsonb_build_object(
            'protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object(
                'code','AGENT_SUCCESSOR_INPUT_INVALID',
                'message','Agent successor trigger set is invalid','status',422
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
        source_manifest_digest,trigger_count
    ) VALUES (
        requested_bot,requested_run,1,batch->>'input_batch_ref',batch->>'input_batch_digest',
        (batch->>'selected_input_end_seq')::bigint,(batch->>'context_revision')::bigint,
        (batch->>'context_view_revision')::bigint,batch->>'source_manifest_digest',
        jsonb_array_length(batch->'triggers')
    );
    FOR trigger_value IN SELECT value FROM jsonb_array_elements(batch->'triggers') LOOP
        INSERT INTO __forest_module_schema__.run_input_triggers(
            bot_id,run_id,input_revision,trigger_index,trigger_ref,principal_ref,ingress_seq
        ) VALUES (
            requested_bot,requested_run,1,trigger_index,trigger_value->>'trigger_ref',
            trigger_value->>'principal_ref',(trigger_value->>'ingress_seq')::bigint
        );
        trigger_index := trigger_index+1;
    END LOOP;
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
