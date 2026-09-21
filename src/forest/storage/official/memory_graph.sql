CREATE TABLE memory_domains (
    bot_id text PRIMARY KEY REFERENCES forest_kernel.bot_identity(bot_id),
    initialization_operation_id text NOT NULL
        CHECK (octet_length(initialization_operation_id) BETWEEN 1 AND 256),
    input_digest text NOT NULL CHECK (input_digest ~ '^sha256:[a-f0-9]{64}$'),
    space_id text NOT NULL CHECK (octet_length(space_id) BETWEEN 1 AND 256),
    embedding_model_kind text NOT NULL
        CHECK (octet_length(embedding_model_kind) BETWEEN 1 AND 256),
    model_revision_digest text NOT NULL CHECK (
        model_revision_digest ~ '^sha256:[a-f0-9]{64}$'
    ),
    dimensions integer NOT NULL CHECK (dimensions BETWEEN 1 AND 4096),
    normalization text NOT NULL CHECK (normalization IN ('L2','NONE')),
    fusion_policy_revision text NOT NULL
        CHECK (octet_length(fusion_policy_revision) BETWEEN 1 AND 256),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (bot_id,initialization_operation_id),
    UNIQUE (bot_id,space_id)
);

CREATE TABLE memory_sources (
    bot_id text NOT NULL REFERENCES forest_kernel.bot_identity(bot_id),
    source_id text NOT NULL CHECK (octet_length(source_id) BETWEEN 1 AND 256),
    registration_operation_id text NOT NULL
        CHECK (octet_length(registration_operation_id) BETWEEN 1 AND 256),
    input_digest text NOT NULL CHECK (input_digest ~ '^sha256:[a-f0-9]{64}$'),
    source_kind text NOT NULL CHECK (
        source_kind IN ('GROUP','DIRECT','DOCUMENT','SYSTEM','UNKNOWN')
    ),
    platform text CHECK (platform IS NULL OR octet_length(platform) BETWEEN 1 AND 64),
    realm_id text CHECK (realm_id IS NULL OR octet_length(realm_id) BETWEEN 1 AND 256),
    external_group_id text CHECK (
        external_group_id IS NULL OR octet_length(external_group_id) BETWEEN 1 AND 256
    ),
    identity_incarnation bigint NOT NULL CHECK (identity_incarnation > 0),
    identity_digest text NOT NULL CHECK (identity_digest ~ '^sha256:[a-f0-9]{64}$'),
    trunk_id text CHECK (trunk_id IS NULL OR octet_length(trunk_id) BETWEEN 1 AND 256),
    status text NOT NULL DEFAULT 'ACTIVE' CHECK (status IN ('ACTIVE','RETIRED')),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (bot_id,source_id),
    UNIQUE (bot_id,registration_operation_id),
    UNIQUE (bot_id,identity_digest),
    CHECK (
        (source_kind='GROUP' AND platform IS NOT NULL AND realm_id IS NOT NULL
            AND external_group_id IS NOT NULL AND trunk_id IS NOT NULL)
        OR (source_kind<>'GROUP' AND external_group_id IS NULL AND trunk_id IS NULL)
    )
);

CREATE TABLE memory_trunks (
    bot_id text NOT NULL,
    trunk_id text NOT NULL CHECK (octet_length(trunk_id) BETWEEN 1 AND 256),
    source_id text NOT NULL,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (bot_id,trunk_id),
    UNIQUE (bot_id,source_id),
    FOREIGN KEY (bot_id,source_id) REFERENCES memory_sources(bot_id,source_id)
);

CREATE TABLE source_registration_operations (
    bot_id text NOT NULL,
    operation_id text NOT NULL CHECK (octet_length(operation_id) BETWEEN 1 AND 256),
    input_digest text NOT NULL CHECK (input_digest ~ '^sha256:[a-f0-9]{64}$'),
    source_id text NOT NULL,
    duplicate_source boolean NOT NULL,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (bot_id,operation_id),
    FOREIGN KEY (bot_id,source_id) REFERENCES memory_sources(bot_id,source_id)
);

CREATE TABLE source_clocks (
    bot_id text NOT NULL,
    source_id text NOT NULL,
    next_source_order_seq bigint NOT NULL DEFAULT 1 CHECK (next_source_order_seq > 0),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (bot_id,source_id),
    FOREIGN KEY (bot_id,source_id) REFERENCES memory_sources(bot_id,source_id)
);

CREATE TABLE source_units (
    bot_id text NOT NULL REFERENCES forest_kernel.bot_identity(bot_id),
    source_unit_id text NOT NULL CHECK (octet_length(source_unit_id) BETWEEN 1 AND 256),
    capture_operation_id text NOT NULL
        CHECK (octet_length(capture_operation_id) BETWEEN 1 AND 256),
    input_digest text NOT NULL CHECK (input_digest ~ '^sha256:[a-f0-9]{64}$'),
    source_id text NOT NULL,
    trunk_id text,
    source_order_seq bigint NOT NULL CHECK (source_order_seq > 0),
    source_revision bigint NOT NULL CHECK (source_revision > 0),
    unit_kind text NOT NULL CHECK (octet_length(unit_kind) BETWEEN 1 AND 128),
    modality text NOT NULL CHECK (modality IN ('TEXT','IMAGE','MIXED','CONTROL')),
    part_count integer NOT NULL CHECK (part_count BETWEEN 1 AND 4096),
    content_manifest_digest text NOT NULL CHECK (
        content_manifest_digest ~ '^sha256:[a-f0-9]{64}$'
    ),
    author_subject_ref text NOT NULL
        CHECK (octet_length(author_subject_ref) BETWEEN 1 AND 256),
    author_payload_id text NOT NULL CHECK (octet_length(author_payload_id) BETWEEN 1 AND 256),
    author_payload_digest text NOT NULL CHECK (
        author_payload_digest ~ '^sha256:[a-f0-9]{64}$'
    ),
    author_owner_component_id text NOT NULL CHECK (
        octet_length(author_owner_component_id) BETWEEN 1 AND 128
    ),
    author_owner_kind text NOT NULL
        CHECK (octet_length(author_owner_kind) BETWEEN 1 AND 128),
    author_owner_id text NOT NULL CHECK (octet_length(author_owner_id) BETWEEN 1 AND 512),
    accepted_deletion_epoch bigint NOT NULL CHECK (accepted_deletion_epoch >= 0),
    eligibility_state text NOT NULL DEFAULT 'REGISTERED' CHECK (
        eligibility_state IN ('REGISTERED','ELIGIBLE','SUPPRESSED')
    ),
    eligibility_revision bigint NOT NULL DEFAULT 0 CHECK (eligibility_revision >= 0),
    eligible_at timestamptz,
    occurred_at timestamptz NOT NULL,
    accepted_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (bot_id,source_unit_id),
    UNIQUE (bot_id,capture_operation_id),
    UNIQUE (bot_id,source_id,source_order_seq),
    FOREIGN KEY (bot_id,source_id) REFERENCES memory_sources(bot_id,source_id),
    FOREIGN KEY (bot_id,trunk_id) REFERENCES memory_trunks(bot_id,trunk_id),
    CHECK (
        (eligibility_state='REGISTERED' AND eligible_at IS NULL)
        OR eligibility_state='SUPPRESSED'
        OR (eligibility_state='ELIGIBLE' AND eligible_at IS NOT NULL)
    )
);

CREATE TABLE source_parts (
    bot_id text NOT NULL,
    source_unit_id text NOT NULL,
    part_id text NOT NULL CHECK (octet_length(part_id) BETWEEN 1 AND 256),
    part_index integer NOT NULL CHECK (part_index BETWEEN 0 AND 4095),
    part_kind text NOT NULL CHECK (part_kind IN ('TEXT','IMAGE')),
    media_type text NOT NULL CHECK (octet_length(media_type) BETWEEN 1 AND 128),
    payload_id text NOT NULL CHECK (octet_length(payload_id) BETWEEN 1 AND 256),
    payload_digest text NOT NULL CHECK (payload_digest ~ '^sha256:[a-f0-9]{64}$'),
    payload_owner_component_id text NOT NULL CHECK (
        octet_length(payload_owner_component_id) BETWEEN 1 AND 128
    ),
    payload_owner_kind text NOT NULL CHECK (
        octet_length(payload_owner_kind) BETWEEN 1 AND 128
    ),
    payload_owner_id text NOT NULL CHECK (octet_length(payload_owner_id) BETWEEN 1 AND 512),
    width integer CHECK (width IS NULL OR width BETWEEN 1 AND 100000),
    height integer CHECK (height IS NULL OR height BETWEEN 1 AND 100000),
    byte_length bigint CHECK (byte_length IS NULL OR byte_length BETWEEN 0 AND 33554432),
    PRIMARY KEY (bot_id,source_unit_id,part_id),
    UNIQUE (bot_id,source_unit_id,part_index),
    FOREIGN KEY (bot_id,source_unit_id) REFERENCES source_units(bot_id,source_unit_id),
    CHECK (
        (part_kind='TEXT' AND width IS NULL AND height IS NULL)
        OR (part_kind='IMAGE' AND width IS NOT NULL AND height IS NOT NULL)
    )
);

CREATE TABLE source_occurrences (
    bot_id text NOT NULL,
    occurrence_id text NOT NULL CHECK (octet_length(occurrence_id) BETWEEN 1 AND 256),
    source_unit_id text NOT NULL,
    binding_ref text NOT NULL CHECK (octet_length(binding_ref) BETWEEN 1 AND 256),
    conversation_ref text NOT NULL
        CHECK (octet_length(conversation_ref) BETWEEN 1 AND 256),
    event_ref text NOT NULL CHECK (octet_length(event_ref) BETWEEN 1 AND 256),
    ingress_seq bigint CHECK (ingress_seq IS NULL OR ingress_seq > 0),
    input_batch_ref text CHECK (
        input_batch_ref IS NULL OR octet_length(input_batch_ref) BETWEEN 1 AND 256
    ),
    context_revision bigint CHECK (context_revision IS NULL OR context_revision > 0),
    eligibility_state text NOT NULL DEFAULT 'REGISTERED' CHECK (
        eligibility_state IN ('REGISTERED','ELIGIBLE','SUPPRESSED')
    ),
    observed_at timestamptz NOT NULL,
    qualified_at timestamptz,
    PRIMARY KEY (bot_id,occurrence_id),
    UNIQUE (bot_id,binding_ref,event_ref),
    UNIQUE (bot_id,conversation_ref,event_ref),
    FOREIGN KEY (bot_id,source_unit_id) REFERENCES source_units(bot_id,source_unit_id),
    CHECK ((input_batch_ref IS NULL) = (context_revision IS NULL)),
    CHECK ((eligibility_state='ELIGIBLE') = (input_batch_ref IS NOT NULL))
);

CREATE TABLE capture_operations (
    bot_id text NOT NULL,
    operation_id text NOT NULL CHECK (octet_length(operation_id) BETWEEN 1 AND 256),
    input_digest text NOT NULL CHECK (input_digest ~ '^sha256:[a-f0-9]{64}$'),
    source_unit_id text NOT NULL,
    occurrence_id text NOT NULL,
    duplicate_source boolean NOT NULL,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (bot_id,operation_id),
    FOREIGN KEY (bot_id,source_unit_id) REFERENCES source_units(bot_id,source_unit_id),
    FOREIGN KEY (bot_id,occurrence_id) REFERENCES source_occurrences(bot_id,occurrence_id)
);

CREATE TABLE eligibility_operations (
    bot_id text NOT NULL,
    operation_id text NOT NULL CHECK (octet_length(operation_id) BETWEEN 1 AND 256),
    input_digest text NOT NULL CHECK (input_digest ~ '^sha256:[a-f0-9]{64}$'),
    source_unit_id text NOT NULL,
    occurrence_id text NOT NULL,
    input_batch_ref text NOT NULL CHECK (octet_length(input_batch_ref) BETWEEN 1 AND 256),
    context_revision bigint NOT NULL CHECK (context_revision > 0),
    resulting_eligibility_revision bigint NOT NULL CHECK (resulting_eligibility_revision > 0),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (bot_id,operation_id),
    FOREIGN KEY (bot_id,source_unit_id) REFERENCES source_units(bot_id,source_unit_id),
    FOREIGN KEY (bot_id,occurrence_id) REFERENCES source_occurrences(bot_id,occurrence_id)
);

CREATE TABLE pending_input_events (
    bot_id text NOT NULL REFERENCES forest_kernel.bot_identity(bot_id),
    conversation_ref text NOT NULL
        CHECK (octet_length(conversation_ref) BETWEEN 1 AND 256),
    event_ref text NOT NULL CHECK (octet_length(event_ref) BETWEEN 1 AND 256),
    operation_id text NOT NULL CHECK (octet_length(operation_id) BETWEEN 1 AND 256),
    input_digest text NOT NULL CHECK (input_digest ~ '^sha256:[a-f0-9]{64}$'),
    input_batch_ref text NOT NULL CHECK (octet_length(input_batch_ref) BETWEEN 1 AND 256),
    context_revision bigint NOT NULL CHECK (context_revision > 0),
    accepted_deletion_epoch bigint NOT NULL CHECK (accepted_deletion_epoch >= 0),
    state text NOT NULL DEFAULT 'PENDING_CAPTURE'
        CHECK (state IN ('PENDING_CAPTURE','RESOLVED','UNAVAILABLE')),
    source_unit_id text,
    occurrence_id text,
    error_code text CHECK (error_code IS NULL OR error_code ~ '^[A-Z][A-Z0-9_]{0,127}$'),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    resolved_at timestamptz,
    PRIMARY KEY (bot_id,conversation_ref,event_ref),
    UNIQUE (bot_id,operation_id,event_ref),
    CHECK (
        (state='PENDING_CAPTURE' AND source_unit_id IS NULL AND occurrence_id IS NULL
            AND error_code IS NULL AND resolved_at IS NULL)
        OR (state='RESOLVED' AND source_unit_id IS NOT NULL AND occurrence_id IS NOT NULL
            AND error_code IS NULL AND resolved_at IS NOT NULL)
        OR (state='UNAVAILABLE' AND source_unit_id IS NULL AND occurrence_id IS NULL
            AND error_code IS NOT NULL AND resolved_at IS NOT NULL)
    ),
    FOREIGN KEY (bot_id,source_unit_id) REFERENCES source_units(bot_id,source_unit_id),
    FOREIGN KEY (bot_id,occurrence_id) REFERENCES source_occurrences(bot_id,occurrence_id)
);

CREATE TABLE source_suppressions (
    bot_id text NOT NULL,
    operation_id text NOT NULL CHECK (octet_length(operation_id) BETWEEN 1 AND 256),
    input_digest text NOT NULL CHECK (input_digest ~ '^sha256:[a-f0-9]{64}$'),
    source_unit_id text NOT NULL,
    deletion_epoch bigint NOT NULL CHECK (deletion_epoch > 0),
    reason_code text NOT NULL CHECK (octet_length(reason_code) BETWEEN 1 AND 128),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (bot_id,operation_id,source_unit_id),
    UNIQUE (bot_id,source_unit_id),
    FOREIGN KEY (bot_id,source_unit_id) REFERENCES source_units(bot_id,source_unit_id)
);

CREATE TABLE relation_contributions (
    bot_id text NOT NULL REFERENCES forest_kernel.bot_identity(bot_id),
    contribution_id text NOT NULL CHECK (octet_length(contribution_id) BETWEEN 1 AND 256),
    operation_id text NOT NULL CHECK (octet_length(operation_id) BETWEEN 1 AND 256),
    input_digest text NOT NULL CHECK (input_digest ~ '^sha256:[a-f0-9]{64}$'),
    relation_id text NOT NULL CHECK (octet_length(relation_id) BETWEEN 1 AND 256),
    relation_kind text NOT NULL CHECK (
        relation_kind IN ('ASSOCIATES_WITH','EVIDENCE','CONTRADICTS','SUBJECT_OF')
    ),
    left_kind text NOT NULL CHECK (left_kind IN ('SOURCE_UNIT','ASSERTION','ENTITY','ARTIFACT')),
    left_id text NOT NULL CHECK (octet_length(left_id) BETWEEN 1 AND 256),
    right_kind text NOT NULL CHECK (right_kind IN ('SOURCE_UNIT','ASSERTION','ENTITY','ARTIFACT')),
    right_id text NOT NULL CHECK (octet_length(right_id) BETWEEN 1 AND 256),
    relation_key text NOT NULL DEFAULT '' CHECK (octet_length(relation_key) <= 256),
    origin_kind text NOT NULL CHECK (origin_kind IN ('MANUAL','VECTOR','RULE','MODEL')),
    weight_micros bigint NOT NULL CHECK (weight_micros BETWEEN 0 AND 1000000000000),
    admitted_deletion_epoch bigint NOT NULL CHECK (admitted_deletion_epoch >= 0),
    active boolean NOT NULL DEFAULT true,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (bot_id,contribution_id),
    UNIQUE (bot_id,operation_id),
    UNIQUE (bot_id,relation_id,contribution_id),
    CHECK (ROW(left_kind,left_id) IS DISTINCT FROM ROW(right_kind,right_id))
);

CREATE TABLE relation_evidence (
    bot_id text NOT NULL,
    contribution_id text NOT NULL,
    evidence_index integer NOT NULL CHECK (evidence_index >= 0),
    source_unit_id text NOT NULL,
    source_revision bigint NOT NULL CHECK (source_revision > 0),
    part_id text NOT NULL CHECK (octet_length(part_id) BETWEEN 1 AND 256),
    PRIMARY KEY (bot_id,contribution_id,evidence_index),
    UNIQUE (bot_id,contribution_id,source_unit_id,source_revision,part_id),
    FOREIGN KEY (bot_id,contribution_id)
        REFERENCES relation_contributions(bot_id,contribution_id),
    FOREIGN KEY (bot_id,source_unit_id) REFERENCES source_units(bot_id,source_unit_id)
);

CREATE TABLE relation_retractions (
    bot_id text NOT NULL,
    operation_id text NOT NULL CHECK (octet_length(operation_id) BETWEEN 1 AND 256),
    input_digest text NOT NULL CHECK (input_digest ~ '^sha256:[a-f0-9]{64}$'),
    contribution_id text NOT NULL,
    reason_payload_id text NOT NULL CHECK (octet_length(reason_payload_id) BETWEEN 1 AND 256),
    reason_payload_digest text NOT NULL CHECK (
        reason_payload_digest ~ '^sha256:[a-f0-9]{64}$'
    ),
    reason_owner_component_id text NOT NULL CHECK (
        octet_length(reason_owner_component_id) BETWEEN 1 AND 128
    ),
    reason_owner_kind text NOT NULL CHECK (
        octet_length(reason_owner_kind) BETWEEN 1 AND 128
    ),
    reason_owner_id text NOT NULL CHECK (octet_length(reason_owner_id) BETWEEN 1 AND 512),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (bot_id,operation_id),
    UNIQUE (bot_id,contribution_id),
    FOREIGN KEY (bot_id,contribution_id)
        REFERENCES relation_contributions(bot_id,contribution_id)
);

CREATE TABLE memory_queries (
    bot_id text NOT NULL REFERENCES forest_kernel.bot_identity(bot_id),
    query_id text NOT NULL CHECK (octet_length(query_id) BETWEEN 1 AND 256),
    operation_id text NOT NULL CHECK (octet_length(operation_id) BETWEEN 1 AND 256),
    input_digest text NOT NULL CHECK (input_digest ~ '^sha256:[a-f0-9]{64}$'),
    current_source_id text NOT NULL,
    input_batch_ref text NOT NULL CHECK (octet_length(input_batch_ref) BETWEEN 1 AND 256),
    context_revision bigint NOT NULL CHECK (context_revision > 0),
    query_mode text NOT NULL CHECK (query_mode IN ('TEXT','IMAGE','MIXED')),
    fusion_policy_revision text NOT NULL
        CHECK (octet_length(fusion_policy_revision) BETWEEN 1 AND 256),
    accepted_deletion_epoch bigint NOT NULL CHECK (accepted_deletion_epoch >= 0),
    state text NOT NULL DEFAULT 'OPEN' CHECK (state IN ('OPEN','FROZEN')),
    bundle_id text CHECK (bundle_id IS NULL OR octet_length(bundle_id) BETWEEN 1 AND 256),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    frozen_at timestamptz,
    PRIMARY KEY (bot_id,query_id),
    UNIQUE (bot_id,operation_id),
    UNIQUE (bot_id,bundle_id),
    FOREIGN KEY (bot_id,current_source_id) REFERENCES memory_sources(bot_id,source_id),
    CHECK ((state='FROZEN') = (bundle_id IS NOT NULL)),
    CHECK ((state='FROZEN') = (frozen_at IS NOT NULL))
);

CREATE TABLE memory_query_parts (
    bot_id text NOT NULL,
    query_id text NOT NULL,
    query_part_index integer NOT NULL CHECK (query_part_index BETWEEN 0 AND 255),
    source_unit_id text NOT NULL,
    source_revision bigint NOT NULL CHECK (source_revision > 0),
    occurrence_id text NOT NULL,
    part_id text NOT NULL CHECK (octet_length(part_id) BETWEEN 1 AND 256),
    modality text NOT NULL CHECK (modality IN ('TEXT','IMAGE')),
    PRIMARY KEY (bot_id,query_id,query_part_index),
    UNIQUE (bot_id,query_id,source_unit_id,part_id),
    FOREIGN KEY (bot_id,query_id) REFERENCES memory_queries(bot_id,query_id),
    FOREIGN KEY (bot_id,source_unit_id,part_id)
        REFERENCES source_parts(bot_id,source_unit_id,part_id),
    FOREIGN KEY (bot_id,occurrence_id) REFERENCES source_occurrences(bot_id,occurrence_id)
);

CREATE TABLE facet_preparation_operations (
    bot_id text NOT NULL,
    operation_id text NOT NULL CHECK (octet_length(operation_id) BETWEEN 1 AND 256),
    input_digest text NOT NULL CHECK (input_digest ~ '^sha256:[a-f0-9]{64}$'),
    target_kind text NOT NULL CHECK (target_kind IN ('SOURCE_UNIT','QUERY')),
    target_id text NOT NULL CHECK (octet_length(target_id) BETWEEN 1 AND 256),
    facet_count integer NOT NULL CHECK (facet_count BETWEEN 1 AND 4096),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (bot_id,operation_id)
);

CREATE TABLE embedding_facets (
    bot_id text NOT NULL REFERENCES forest_kernel.bot_identity(bot_id),
    facet_id text NOT NULL CHECK (octet_length(facet_id) BETWEEN 1 AND 256),
    purpose text NOT NULL CHECK (purpose IN ('DOCUMENT','QUERY')),
    query_id text,
    source_unit_id text NOT NULL,
    source_revision bigint NOT NULL CHECK (source_revision > 0),
    part_id text NOT NULL CHECK (octet_length(part_id) BETWEEN 1 AND 256),
    modality text NOT NULL CHECK (modality IN ('TEXT','IMAGE')),
    selector_start_char integer,
    selector_end_char integer,
    input_payload_id text NOT NULL CHECK (octet_length(input_payload_id) BETWEEN 1 AND 256),
    input_payload_digest text NOT NULL CHECK (
        input_payload_digest ~ '^sha256:[a-f0-9]{64}$'
    ),
    facet_content_digest text NOT NULL CHECK (
        facet_content_digest ~ '^sha256:[a-f0-9]{64}$'
    ),
    input_media_type text NOT NULL CHECK (octet_length(input_media_type) BETWEEN 1 AND 128),
    input_byte_length bigint NOT NULL CHECK (input_byte_length BETWEEN 1 AND 33554432),
    space_id text NOT NULL CHECK (octet_length(space_id) BETWEEN 1 AND 256),
    accepted_deletion_epoch bigint NOT NULL CHECK (accepted_deletion_epoch >= 0),
    state text NOT NULL DEFAULT 'PENDING' CHECK (
        state IN ('PENDING','PREPARED','DISPATCHED','EMBEDDED','READY','UNKNOWN','FAILED','SUPPRESSED')
    ),
    attempt_count integer NOT NULL DEFAULT 0 CHECK (attempt_count >= 0),
    current_attempt_id text,
    index_ref_id text CHECK (index_ref_id IS NULL OR octet_length(index_ref_id) BETWEEN 1 AND 256),
    vector_digest text CHECK (vector_digest IS NULL OR vector_digest ~ '^sha256:[a-f0-9]{64}$'),
    result_payload_id text,
    result_payload_digest text CHECK (
        result_payload_digest IS NULL OR result_payload_digest ~ '^sha256:[a-f0-9]{64}$'
    ),
    published_at timestamptz,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (bot_id,facet_id),
    FOREIGN KEY (bot_id,query_id) REFERENCES memory_queries(bot_id,query_id),
    FOREIGN KEY (bot_id,source_unit_id,part_id)
        REFERENCES source_parts(bot_id,source_unit_id,part_id),
    CHECK ((purpose='QUERY') = (query_id IS NOT NULL)),
    CHECK (
        (modality='TEXT' AND selector_start_char IS NOT NULL
            AND selector_end_char IS NOT NULL AND selector_start_char < selector_end_char)
        OR (modality='IMAGE' AND selector_start_char IS NULL AND selector_end_char IS NULL)
    ),
    CHECK ((state='READY') = (index_ref_id IS NOT NULL)),
    CHECK ((state IN ('EMBEDDED','READY')) = (vector_digest IS NOT NULL)),
    CHECK ((state IN ('EMBEDDED','READY')) = (result_payload_id IS NOT NULL)),
    CHECK ((state IN ('EMBEDDED','READY')) = (result_payload_digest IS NOT NULL)),
    CHECK ((state='READY') = (published_at IS NOT NULL)),
    UNIQUE (bot_id,index_ref_id)
);

CREATE TABLE embedding_attempts (
    bot_id text NOT NULL,
    attempt_id text NOT NULL CHECK (octet_length(attempt_id) BETWEEN 1 AND 256),
    operation_id text NOT NULL CHECK (octet_length(operation_id) BETWEEN 1 AND 256),
    input_digest text NOT NULL CHECK (input_digest ~ '^sha256:[a-f0-9]{64}$'),
    facet_id text NOT NULL,
    attempt_no integer NOT NULL CHECK (attempt_no > 0),
    executor_component_id text NOT NULL
        CHECK (octet_length(executor_component_id) BETWEEN 1 AND 128),
    executor_generation_id text NOT NULL
        CHECK (octet_length(executor_generation_id) BETWEEN 1 AND 256),
    route_revision text NOT NULL CHECK (octet_length(route_revision) BETWEEN 1 AND 256),
    accepted_deletion_epoch bigint NOT NULL CHECK (accepted_deletion_epoch >= 0),
    deadline_at timestamptz NOT NULL,
    state text NOT NULL DEFAULT 'PREPARED' CHECK (
        state IN ('PREPARED','DISPATCHED','SUCCEEDED','NOT_SENT','UNKNOWN','RETRYABLE_FAILURE','FAILED')
    ),
    handoff_operation_id text,
    settlement_operation_id text CHECK (
        settlement_operation_id IS NULL
        OR octet_length(settlement_operation_id) BETWEEN 1 AND 256
    ),
    result_payload_id text,
    result_payload_digest text CHECK (
        result_payload_digest IS NULL OR result_payload_digest ~ '^sha256:[a-f0-9]{64}$'
    ),
    vector_digest text CHECK (vector_digest IS NULL OR vector_digest ~ '^sha256:[a-f0-9]{64}$'),
    error_code text CHECK (error_code IS NULL OR octet_length(error_code) BETWEEN 1 AND 128),
    prepared_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    dispatched_at timestamptz,
    settled_at timestamptz,
    PRIMARY KEY (bot_id,attempt_id),
    UNIQUE (bot_id,operation_id),
    UNIQUE (bot_id,facet_id,attempt_no),
    UNIQUE (bot_id,attempt_id,facet_id),
    FOREIGN KEY (bot_id,facet_id) REFERENCES embedding_facets(bot_id,facet_id),
    CHECK (state<>'PREPARED' OR handoff_operation_id IS NULL),
    CHECK (state<>'PREPARED' OR dispatched_at IS NULL),
    CHECK (
        state NOT IN ('DISPATCHED','SUCCEEDED','UNKNOWN','RETRYABLE_FAILURE','FAILED')
        OR handoff_operation_id IS NOT NULL
    ),
    CHECK (
        state NOT IN ('DISPATCHED','SUCCEEDED','UNKNOWN','RETRYABLE_FAILURE','FAILED')
        OR dispatched_at IS NOT NULL
    ),
    CHECK ((state IN ('PREPARED','DISPATCHED')) = (settlement_operation_id IS NULL)),
    CHECK ((state IN ('PREPARED','DISPATCHED')) = (settled_at IS NULL)),
    CHECK ((state='SUCCEEDED') = (result_payload_id IS NOT NULL)),
    CHECK ((state='SUCCEEDED') = (result_payload_digest IS NOT NULL)),
    CHECK ((state='SUCCEEDED') = (vector_digest IS NOT NULL))
);

CREATE TABLE embedding_claims (
    bot_id text NOT NULL REFERENCES forest_kernel.bot_identity(bot_id),
    claim_id text NOT NULL CHECK (octet_length(claim_id) BETWEEN 1 AND 256),
    operation_id text NOT NULL CHECK (octet_length(operation_id) BETWEEN 1 AND 256),
    input_digest text NOT NULL CHECK (input_digest ~ '^sha256:[a-f0-9]{64}$'),
    purpose text NOT NULL CHECK (purpose IN ('DOCUMENT','QUERY')),
    executor_component_id text NOT NULL
        CHECK (octet_length(executor_component_id) BETWEEN 1 AND 128),
    executor_generation_id text NOT NULL
        CHECK (octet_length(executor_generation_id) BETWEEN 1 AND 256),
    route_revision text NOT NULL CHECK (octet_length(route_revision) BETWEEN 1 AND 256),
    accepted_deletion_epoch bigint NOT NULL CHECK (accepted_deletion_epoch >= 0),
    deadline_at timestamptz NOT NULL,
    item_limit integer NOT NULL CHECK (item_limit BETWEEN 1 AND 32),
    item_count integer NOT NULL CHECK (item_count BETWEEN 0 AND item_limit),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (bot_id,claim_id),
    UNIQUE (bot_id,operation_id)
);

CREATE TABLE embedding_claim_items (
    bot_id text NOT NULL,
    claim_id text NOT NULL,
    item_index integer NOT NULL CHECK (item_index BETWEEN 0 AND 31),
    facet_id text NOT NULL,
    attempt_id text NOT NULL,
    PRIMARY KEY (bot_id,claim_id,item_index),
    UNIQUE (bot_id,attempt_id),
    FOREIGN KEY (bot_id,claim_id) REFERENCES embedding_claims(bot_id,claim_id),
    FOREIGN KEY (bot_id,attempt_id,facet_id)
        REFERENCES embedding_attempts(bot_id,attempt_id,facet_id)
);

CREATE TABLE embedding_budget_days (
    bot_id text NOT NULL REFERENCES forest_kernel.bot_identity(bot_id),
    budget_day date NOT NULL,
    reserved_requests integer NOT NULL DEFAULT 0 CHECK (reserved_requests >= 0),
    settled_requests bigint NOT NULL DEFAULT 0 CHECK (settled_requests >= 0),
    unknown_requests bigint NOT NULL DEFAULT 0 CHECK (unknown_requests >= 0),
    reserved_tokens bigint NOT NULL DEFAULT 0 CHECK (reserved_tokens >= 0),
    charged_tokens bigint NOT NULL DEFAULT 0 CHECK (charged_tokens >= 0),
    unknown_tokens bigint NOT NULL DEFAULT 0 CHECK (unknown_tokens >= 0),
    revision bigint NOT NULL DEFAULT 0 CHECK (revision >= 0),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (bot_id,budget_day)
);

CREATE TABLE embedding_budget_reservations (
    bot_id text NOT NULL,
    reservation_id text NOT NULL CHECK (octet_length(reservation_id) BETWEEN 1 AND 256),
    operation_id text NOT NULL CHECK (octet_length(operation_id) BETWEEN 1 AND 256),
    input_digest text NOT NULL CHECK (input_digest ~ '^sha256:[a-f0-9]{64}$'),
    batch_ref text NOT NULL CHECK (octet_length(batch_ref) BETWEEN 1 AND 256),
    policy_revision text NOT NULL CHECK (octet_length(policy_revision) BETWEEN 1 AND 256),
    budget_day date NOT NULL,
    request_limit integer NOT NULL CHECK (request_limit BETWEEN 1 AND 1000000),
    token_limit bigint NOT NULL CHECK (token_limit BETWEEN 1 AND 1000000000000),
    reserved_tokens bigint NOT NULL CHECK (reserved_tokens BETWEEN 1 AND 1000000000),
    deadline_at timestamptz NOT NULL,
    state text NOT NULL DEFAULT 'RESERVED' CHECK (
        state IN ('RESERVED','RELEASED','CHARGED','UNKNOWN')
    ),
    settlement_operation_id text,
    charged_tokens bigint CHECK (charged_tokens IS NULL OR charged_tokens >= 0),
    error_code text CHECK (error_code IS NULL OR octet_length(error_code) BETWEEN 1 AND 128),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    settled_at timestamptz,
    PRIMARY KEY (bot_id,reservation_id),
    UNIQUE (bot_id,operation_id),
    UNIQUE (bot_id,settlement_operation_id),
    FOREIGN KEY (bot_id,budget_day) REFERENCES embedding_budget_days(bot_id,budget_day),
    CHECK ((state='RESERVED') = (settled_at IS NULL)),
    CHECK ((state='RESERVED') = (settlement_operation_id IS NULL)),
    CHECK ((state='CHARGED') = (charged_tokens IS NOT NULL))
);

CREATE TABLE retrieval_bundles (
    bot_id text NOT NULL,
    bundle_id text NOT NULL CHECK (octet_length(bundle_id) BETWEEN 1 AND 256),
    query_id text NOT NULL,
    operation_id text NOT NULL CHECK (octet_length(operation_id) BETWEEN 1 AND 256),
    input_digest text NOT NULL CHECK (input_digest ~ '^sha256:[a-f0-9]{64}$'),
    index_snapshot_at timestamptz NOT NULL,
    accepted_deletion_epoch bigint NOT NULL CHECK (accepted_deletion_epoch >= 0),
    result_limit integer NOT NULL CHECK (result_limit BETWEEN 1 AND 256),
    candidate_limit_per_facet integer NOT NULL CHECK (
        candidate_limit_per_facet BETWEEN 1 AND 256
    ),
    result_count integer NOT NULL CHECK (result_count BETWEEN 0 AND 256),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (bot_id,bundle_id),
    UNIQUE (bot_id,query_id),
    UNIQUE (bot_id,operation_id),
    FOREIGN KEY (bot_id,query_id) REFERENCES memory_queries(bot_id,query_id)
);

CREATE TABLE retrieval_items (
    bot_id text NOT NULL,
    bundle_id text NOT NULL,
    item_index integer NOT NULL CHECK (item_index BETWEEN 0 AND 255),
    source_unit_id text NOT NULL,
    source_revision bigint NOT NULL CHECK (source_revision > 0),
    rank_micros bigint NOT NULL CHECK (rank_micros >= 0),
    PRIMARY KEY (bot_id,bundle_id,item_index),
    UNIQUE (bot_id,bundle_id,source_unit_id),
    FOREIGN KEY (bot_id,bundle_id) REFERENCES retrieval_bundles(bot_id,bundle_id),
    FOREIGN KEY (bot_id,source_unit_id) REFERENCES source_units(bot_id,source_unit_id)
);

CREATE TABLE retrieval_matches (
    bot_id text NOT NULL,
    bundle_id text NOT NULL,
    item_index integer NOT NULL,
    match_index integer NOT NULL CHECK (match_index BETWEEN 0 AND 4095),
    query_facet_id text NOT NULL,
    document_facet_id text NOT NULL,
    query_modality text NOT NULL CHECK (query_modality IN ('TEXT','IMAGE')),
    document_modality text NOT NULL CHECK (document_modality IN ('TEXT','IMAGE')),
    candidate_rank integer NOT NULL CHECK (candidate_rank BETWEEN 1 AND 256),
    score real NOT NULL CHECK (score = score),
    PRIMARY KEY (bot_id,bundle_id,item_index,match_index),
    FOREIGN KEY (bot_id,bundle_id,item_index)
        REFERENCES retrieval_items(bot_id,bundle_id,item_index),
    FOREIGN KEY (bot_id,query_facet_id) REFERENCES embedding_facets(bot_id,facet_id),
    FOREIGN KEY (bot_id,document_facet_id) REFERENCES embedding_facets(bot_id,facet_id)
);

CREATE INDEX sources_external_group_idx ON memory_sources(
    bot_id,platform,realm_id,external_group_id,identity_incarnation
);
CREATE UNIQUE INDEX sources_group_identity_uniq ON memory_sources(
    bot_id,platform,realm_id,external_group_id,identity_incarnation
) WHERE source_kind='GROUP';
CREATE INDEX source_units_browse_idx ON source_units(
    bot_id,eligibility_state,occurred_at DESC,source_unit_id DESC
);
CREATE INDEX source_units_trunk_idx ON source_units(
    bot_id,trunk_id,eligibility_state,occurred_at DESC,source_unit_id DESC
);

CREATE TABLE vector_index_deletions (
    bot_id text NOT NULL REFERENCES forest_kernel.bot_identity(bot_id),
    index_ref_id text NOT NULL CHECK (octet_length(index_ref_id) BETWEEN 1 AND 256),
    space_id text NOT NULL CHECK (octet_length(space_id) BETWEEN 1 AND 256),
    source_unit_id text NOT NULL CHECK (octet_length(source_unit_id) BETWEEN 1 AND 256),
    deletion_epoch bigint NOT NULL CHECK (deletion_epoch >= 0),
    state text NOT NULL DEFAULT 'PENDING' CHECK (state IN ('PENDING','DELETED')),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    deleted_at timestamptz,
    PRIMARY KEY (bot_id,index_ref_id),
    CHECK ((state='DELETED') = (deleted_at IS NOT NULL))
);
CREATE INDEX source_units_author_idx ON source_units(
    bot_id,author_subject_ref,eligibility_state,occurred_at DESC,source_unit_id DESC
);
CREATE INDEX source_units_modality_idx ON source_units(
    bot_id,modality,eligibility_state,occurred_at DESC,source_unit_id DESC
);
CREATE INDEX source_units_kind_idx ON source_units(
    bot_id,unit_kind,eligibility_state,occurred_at DESC,source_unit_id DESC
);
CREATE INDEX relation_left_idx ON relation_contributions(
    bot_id,left_kind,left_id,active,relation_id,contribution_id
);
CREATE INDEX relation_right_idx ON relation_contributions(
    bot_id,right_kind,right_id,active,relation_id,contribution_id
);
CREATE INDEX embedding_facets_work_idx ON embedding_facets(
    bot_id,state,purpose,created_at,facet_id
);
CREATE INDEX embedding_facets_source_idx ON embedding_facets(
    bot_id,source_unit_id,source_revision,part_id,purpose,state
);
CREATE INDEX embedding_facets_query_idx ON embedding_facets(
    bot_id,query_id,purpose,state,facet_id
);
CREATE INDEX embedding_facets_snapshot_idx ON embedding_facets(
    bot_id,space_id,purpose,state,published_at,facet_id
);
CREATE INDEX embedding_attempts_recovery_idx ON embedding_attempts(
    bot_id,state,deadline_at,attempt_id
);
CREATE INDEX vector_index_deletions_work_idx ON vector_index_deletions(
    bot_id,state,created_at,index_ref_id
);
CREATE INDEX embedding_claims_created_idx ON embedding_claims(
    bot_id,purpose,created_at,claim_id
);
CREATE INDEX embedding_budget_reservations_state_idx ON embedding_budget_reservations(
    bot_id,state,budget_day,reservation_id
);

CREATE FUNCTION reject_immutable_row() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    RAISE EXCEPTION 'memory.graph immutable fact cannot change' USING ERRCODE='55000';
END;
$$;

CREATE FUNCTION guard_memory_domain() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    RAISE EXCEPTION 'Memory embedding space is bound to the Bot lifecycle' USING ERRCODE='55000';
END;
$$;

CREATE FUNCTION guard_memory_source() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF ROW(NEW.bot_id,NEW.source_id,NEW.registration_operation_id,NEW.input_digest,
           NEW.source_kind,NEW.platform,NEW.realm_id,NEW.external_group_id,
           NEW.identity_incarnation,NEW.identity_digest,NEW.trunk_id,NEW.created_at)
       IS DISTINCT FROM
       ROW(OLD.bot_id,OLD.source_id,OLD.registration_operation_id,OLD.input_digest,
           OLD.source_kind,OLD.platform,OLD.realm_id,OLD.external_group_id,
           OLD.identity_incarnation,OLD.identity_digest,OLD.trunk_id,OLD.created_at) THEN
        RAISE EXCEPTION 'Memory source identity is immutable' USING ERRCODE='55000';
    END IF;
    IF OLD.status='ACTIVE' AND NEW.status='RETIRED' THEN RETURN NEW; END IF;
    IF NEW IS NOT DISTINCT FROM OLD THEN RETURN NEW; END IF;
    RAISE EXCEPTION 'Invalid Memory source transition' USING ERRCODE='55000';
END;
$$;

CREATE FUNCTION guard_source_unit() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF ROW(NEW.bot_id,NEW.source_unit_id,NEW.capture_operation_id,NEW.input_digest,
           NEW.source_id,NEW.trunk_id,NEW.source_order_seq,NEW.source_revision,
           NEW.unit_kind,NEW.modality,NEW.part_count,NEW.content_manifest_digest,
           NEW.author_subject_ref,NEW.author_payload_id,NEW.author_payload_digest,
           NEW.author_owner_component_id,NEW.author_owner_kind,NEW.author_owner_id,
           NEW.accepted_deletion_epoch,NEW.occurred_at,NEW.accepted_at)
       IS DISTINCT FROM
       ROW(OLD.bot_id,OLD.source_unit_id,OLD.capture_operation_id,OLD.input_digest,
           OLD.source_id,OLD.trunk_id,OLD.source_order_seq,OLD.source_revision,
           OLD.unit_kind,OLD.modality,OLD.part_count,OLD.content_manifest_digest,
           OLD.author_subject_ref,OLD.author_payload_id,OLD.author_payload_digest,
           OLD.author_owner_component_id,OLD.author_owner_kind,OLD.author_owner_id,
           OLD.accepted_deletion_epoch,OLD.occurred_at,OLD.accepted_at) THEN
        RAISE EXCEPTION 'Memory source unit identity is immutable' USING ERRCODE='55000';
    END IF;
    IF OLD.eligibility_state='REGISTERED' AND NEW.eligibility_state IN ('ELIGIBLE','SUPPRESSED')
       AND NEW.eligibility_revision=OLD.eligibility_revision+1 THEN RETURN NEW; END IF;
    IF OLD.eligibility_state='ELIGIBLE' AND NEW.eligibility_state='SUPPRESSED'
       AND NEW.eligibility_revision=OLD.eligibility_revision+1 THEN RETURN NEW; END IF;
    IF NEW IS NOT DISTINCT FROM OLD THEN RETURN NEW; END IF;
    RAISE EXCEPTION 'Invalid Memory eligibility transition' USING ERRCODE='55000';
END;
$$;

CREATE FUNCTION guard_occurrence() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF ROW(NEW.bot_id,NEW.occurrence_id,NEW.source_unit_id,NEW.binding_ref,
           NEW.conversation_ref,NEW.event_ref,NEW.ingress_seq,NEW.observed_at)
       IS DISTINCT FROM
       ROW(OLD.bot_id,OLD.occurrence_id,OLD.source_unit_id,OLD.binding_ref,
           OLD.conversation_ref,OLD.event_ref,OLD.ingress_seq,OLD.observed_at) THEN
        RAISE EXCEPTION 'Memory occurrence identity is immutable' USING ERRCODE='55000';
    END IF;
    IF OLD.eligibility_state='REGISTERED' AND NEW.eligibility_state IN ('ELIGIBLE','SUPPRESSED')
       THEN RETURN NEW; END IF;
    IF OLD.eligibility_state='ELIGIBLE' AND NEW.eligibility_state='SUPPRESSED'
       THEN RETURN NEW; END IF;
    IF NEW IS NOT DISTINCT FROM OLD THEN RETURN NEW; END IF;
    RAISE EXCEPTION 'Invalid Memory occurrence transition' USING ERRCODE='55000';
END;
$$;

CREATE FUNCTION guard_relation_contribution() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF ROW(NEW.bot_id,NEW.contribution_id,NEW.operation_id,NEW.input_digest,
           NEW.relation_id,NEW.relation_kind,NEW.left_kind,NEW.left_id,
           NEW.right_kind,NEW.right_id,NEW.relation_key,NEW.origin_kind,
           NEW.weight_micros,NEW.admitted_deletion_epoch,NEW.created_at)
       IS DISTINCT FROM
       ROW(OLD.bot_id,OLD.contribution_id,OLD.operation_id,OLD.input_digest,
           OLD.relation_id,OLD.relation_kind,OLD.left_kind,OLD.left_id,
           OLD.right_kind,OLD.right_id,OLD.relation_key,OLD.origin_kind,
           OLD.weight_micros,OLD.admitted_deletion_epoch,OLD.created_at) THEN
        RAISE EXCEPTION 'Memory relation contribution identity is immutable' USING ERRCODE='55000';
    END IF;
    IF OLD.active AND NOT NEW.active THEN RETURN NEW; END IF;
    IF NEW IS NOT DISTINCT FROM OLD THEN RETURN NEW; END IF;
    RAISE EXCEPTION 'Invalid Memory relation transition' USING ERRCODE='55000';
END;
$$;

CREATE FUNCTION guard_memory_query() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF ROW(NEW.bot_id,NEW.query_id,NEW.operation_id,NEW.input_digest,
           NEW.current_source_id,NEW.input_batch_ref,NEW.context_revision,
           NEW.query_mode,NEW.fusion_policy_revision,NEW.accepted_deletion_epoch,
           NEW.created_at)
       IS DISTINCT FROM
       ROW(OLD.bot_id,OLD.query_id,OLD.operation_id,OLD.input_digest,
           OLD.current_source_id,OLD.input_batch_ref,OLD.context_revision,
           OLD.query_mode,OLD.fusion_policy_revision,OLD.accepted_deletion_epoch,
           OLD.created_at) THEN
        RAISE EXCEPTION 'Memory query identity is immutable' USING ERRCODE='55000';
    END IF;
    IF OLD.state='OPEN' AND NEW.state='FROZEN' THEN RETURN NEW; END IF;
    IF NEW IS NOT DISTINCT FROM OLD THEN RETURN NEW; END IF;
    RAISE EXCEPTION 'Invalid Memory query transition' USING ERRCODE='55000';
END;
$$;

CREATE FUNCTION guard_embedding_facet() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF ROW(NEW.bot_id,NEW.facet_id,NEW.purpose,NEW.query_id,NEW.source_unit_id,
           NEW.source_revision,NEW.part_id,NEW.modality,NEW.selector_start_char,
           NEW.selector_end_char,NEW.input_payload_id,NEW.input_payload_digest,
           NEW.facet_content_digest,NEW.input_media_type,NEW.input_byte_length,
           NEW.space_id,NEW.accepted_deletion_epoch,NEW.created_at)
       IS DISTINCT FROM
       ROW(OLD.bot_id,OLD.facet_id,OLD.purpose,OLD.query_id,OLD.source_unit_id,
           OLD.source_revision,OLD.part_id,OLD.modality,OLD.selector_start_char,
           OLD.selector_end_char,OLD.input_payload_id,OLD.input_payload_digest,
           OLD.facet_content_digest,OLD.input_media_type,OLD.input_byte_length,
           OLD.space_id,OLD.accepted_deletion_epoch,OLD.created_at) THEN
        RAISE EXCEPTION 'Embedding facet identity is immutable' USING ERRCODE='55000';
    END IF;
    IF OLD.state='PENDING' AND NEW.state IN ('PREPARED','SUPPRESSED') THEN RETURN NEW; END IF;
    IF OLD.state='PREPARED' AND NEW.state IN ('DISPATCHED','PENDING','SUPPRESSED') THEN RETURN NEW; END IF;
    IF OLD.state='DISPATCHED' AND NEW.state IN (
        'EMBEDDED','UNKNOWN','FAILED','PENDING','SUPPRESSED'
    ) THEN RETURN NEW; END IF;
    IF OLD.state='EMBEDDED' AND NEW.state IN ('READY','SUPPRESSED') THEN RETURN NEW; END IF;
    IF OLD.state IN ('READY','UNKNOWN','FAILED') AND NEW.state='SUPPRESSED' THEN RETURN NEW; END IF;
    IF NEW IS NOT DISTINCT FROM OLD THEN RETURN NEW; END IF;
    RAISE EXCEPTION 'Invalid embedding facet transition' USING ERRCODE='55000';
END;
$$;

CREATE FUNCTION guard_embedding_attempt() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF ROW(NEW.bot_id,NEW.attempt_id,NEW.operation_id,NEW.input_digest,NEW.facet_id,
           NEW.attempt_no,NEW.executor_component_id,NEW.executor_generation_id,
           NEW.route_revision,NEW.accepted_deletion_epoch,NEW.deadline_at,NEW.prepared_at)
       IS DISTINCT FROM
       ROW(OLD.bot_id,OLD.attempt_id,OLD.operation_id,OLD.input_digest,OLD.facet_id,
           OLD.attempt_no,OLD.executor_component_id,OLD.executor_generation_id,
           OLD.route_revision,OLD.accepted_deletion_epoch,OLD.deadline_at,OLD.prepared_at) THEN
        RAISE EXCEPTION 'Embedding attempt identity is immutable' USING ERRCODE='55000';
    END IF;
    IF OLD.state='PREPARED' AND NEW.state IN ('DISPATCHED','NOT_SENT') THEN RETURN NEW; END IF;
    IF OLD.state='DISPATCHED' AND NEW.state IN (
        'SUCCEEDED','NOT_SENT','UNKNOWN','RETRYABLE_FAILURE','FAILED'
    ) THEN RETURN NEW; END IF;
    IF NEW IS NOT DISTINCT FROM OLD THEN RETURN NEW; END IF;
    RAISE EXCEPTION 'Invalid embedding attempt transition' USING ERRCODE='55000';
END;
$$;

CREATE FUNCTION guard_vector_index_deletion() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF ROW(NEW.bot_id,NEW.index_ref_id,NEW.space_id,NEW.source_unit_id,
           NEW.deletion_epoch,NEW.created_at)
       IS DISTINCT FROM
       ROW(OLD.bot_id,OLD.index_ref_id,OLD.space_id,OLD.source_unit_id,
           OLD.deletion_epoch,OLD.created_at) THEN
        RAISE EXCEPTION 'Vector index deletion identity is immutable' USING ERRCODE='55000';
    END IF;
    IF OLD.state='PENDING' AND NEW.state='DELETED' AND NEW.deleted_at IS NOT NULL THEN
        RETURN NEW;
    END IF;
    IF NEW IS NOT DISTINCT FROM OLD THEN RETURN NEW; END IF;
    RAISE EXCEPTION 'Invalid vector index deletion transition' USING ERRCODE='55000';
END;
$$;

CREATE FUNCTION guard_embedding_budget_reservation() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF ROW(NEW.bot_id,NEW.reservation_id,NEW.operation_id,NEW.input_digest,
           NEW.batch_ref,NEW.policy_revision,NEW.budget_day,NEW.request_limit,
           NEW.token_limit,NEW.reserved_tokens,NEW.deadline_at,NEW.created_at)
       IS DISTINCT FROM
       ROW(OLD.bot_id,OLD.reservation_id,OLD.operation_id,OLD.input_digest,
           OLD.batch_ref,OLD.policy_revision,OLD.budget_day,OLD.request_limit,
           OLD.token_limit,OLD.reserved_tokens,OLD.deadline_at,OLD.created_at) THEN
        RAISE EXCEPTION 'Embedding budget reservation identity is immutable' USING ERRCODE='55000';
    END IF;
    IF OLD.state='RESERVED' AND NEW.state IN ('RELEASED','CHARGED','UNKNOWN') THEN
        RETURN NEW;
    END IF;
    IF NEW IS NOT DISTINCT FROM OLD THEN RETURN NEW; END IF;
    RAISE EXCEPTION 'Invalid embedding budget transition' USING ERRCODE='55000';
END;
$$;

CREATE TRIGGER memory_domain_immutable BEFORE UPDATE OR DELETE ON memory_domains
    FOR EACH ROW EXECUTE FUNCTION guard_memory_domain();
CREATE TRIGGER memory_source_guard BEFORE UPDATE OR DELETE ON memory_sources
    FOR EACH ROW EXECUTE FUNCTION guard_memory_source();
CREATE TRIGGER source_unit_guard BEFORE UPDATE OR DELETE ON source_units
    FOR EACH ROW EXECUTE FUNCTION guard_source_unit();
CREATE TRIGGER source_occurrence_guard BEFORE UPDATE OR DELETE ON source_occurrences
    FOR EACH ROW EXECUTE FUNCTION guard_occurrence();
CREATE TRIGGER relation_contribution_guard BEFORE UPDATE OR DELETE ON relation_contributions
    FOR EACH ROW EXECUTE FUNCTION guard_relation_contribution();
CREATE TRIGGER memory_query_guard BEFORE UPDATE OR DELETE ON memory_queries
    FOR EACH ROW EXECUTE FUNCTION guard_memory_query();
CREATE TRIGGER embedding_facet_guard BEFORE UPDATE OR DELETE ON embedding_facets
    FOR EACH ROW EXECUTE FUNCTION guard_embedding_facet();
CREATE TRIGGER embedding_attempt_guard BEFORE UPDATE OR DELETE ON embedding_attempts
    FOR EACH ROW EXECUTE FUNCTION guard_embedding_attempt();
CREATE TRIGGER vector_index_deletion_guard BEFORE UPDATE OR DELETE ON vector_index_deletions
    FOR EACH ROW EXECUTE FUNCTION guard_vector_index_deletion();
CREATE TRIGGER embedding_claims_immutable BEFORE UPDATE OR DELETE ON embedding_claims
    FOR EACH ROW EXECUTE FUNCTION reject_immutable_row();
CREATE TRIGGER embedding_claim_items_immutable BEFORE UPDATE OR DELETE ON embedding_claim_items
    FOR EACH ROW EXECUTE FUNCTION reject_immutable_row();
CREATE TRIGGER embedding_budget_reservation_guard
    BEFORE UPDATE OR DELETE ON embedding_budget_reservations
    FOR EACH ROW EXECUTE FUNCTION guard_embedding_budget_reservation();
CREATE TRIGGER memory_trunks_immutable BEFORE UPDATE OR DELETE ON memory_trunks
    FOR EACH ROW EXECUTE FUNCTION reject_immutable_row();
CREATE TRIGGER source_registration_operations_immutable
    BEFORE UPDATE OR DELETE ON source_registration_operations
    FOR EACH ROW EXECUTE FUNCTION reject_immutable_row();
CREATE TRIGGER source_parts_immutable BEFORE UPDATE OR DELETE ON source_parts
    FOR EACH ROW EXECUTE FUNCTION reject_immutable_row();
CREATE TRIGGER capture_operations_immutable BEFORE UPDATE OR DELETE ON capture_operations
    FOR EACH ROW EXECUTE FUNCTION reject_immutable_row();
CREATE TRIGGER eligibility_operations_immutable BEFORE UPDATE OR DELETE ON eligibility_operations
    FOR EACH ROW EXECUTE FUNCTION reject_immutable_row();
CREATE TRIGGER source_suppressions_immutable BEFORE UPDATE OR DELETE ON source_suppressions
    FOR EACH ROW EXECUTE FUNCTION reject_immutable_row();
CREATE TRIGGER relation_evidence_immutable BEFORE UPDATE OR DELETE ON relation_evidence
    FOR EACH ROW EXECUTE FUNCTION reject_immutable_row();
CREATE TRIGGER relation_retractions_immutable BEFORE UPDATE OR DELETE ON relation_retractions
    FOR EACH ROW EXECUTE FUNCTION reject_immutable_row();
CREATE TRIGGER memory_query_parts_immutable BEFORE UPDATE OR DELETE ON memory_query_parts
    FOR EACH ROW EXECUTE FUNCTION reject_immutable_row();
CREATE TRIGGER facet_preparation_operations_immutable
    BEFORE UPDATE OR DELETE ON facet_preparation_operations
    FOR EACH ROW EXECUTE FUNCTION reject_immutable_row();
CREATE TRIGGER retrieval_bundles_immutable BEFORE UPDATE OR DELETE ON retrieval_bundles
    FOR EACH ROW EXECUTE FUNCTION reject_immutable_row();
CREATE TRIGGER retrieval_items_immutable BEFORE UPDATE OR DELETE ON retrieval_items
    FOR EACH ROW EXECUTE FUNCTION reject_immutable_row();
CREATE TRIGGER retrieval_matches_immutable BEFORE UPDATE OR DELETE ON retrieval_matches
    FOR EACH ROW EXECUTE FUNCTION reject_immutable_row();
