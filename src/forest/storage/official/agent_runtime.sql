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
CREATE TRIGGER model_turns_immutable BEFORE UPDATE OR DELETE ON model_turns
    FOR EACH ROW EXECUTE FUNCTION reject_agent_fact_change();
CREATE TRIGGER tool_observations_immutable BEFORE UPDATE OR DELETE ON tool_observations
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
