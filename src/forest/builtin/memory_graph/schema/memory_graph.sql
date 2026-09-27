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

CREATE TABLE memory_domain_bind_work (
    bot_id text PRIMARY KEY REFERENCES forest_kernel.bot_identity(bot_id),
    state text NOT NULL CHECK (state IN ('PENDING','CLAIMED','READY')),
    claim_token text UNIQUE,
    lease_deadline timestamptz,
    attempt_count integer NOT NULL DEFAULT 0 CHECK (attempt_count >= 0),
    next_attempt_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    last_error_code text,
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    CHECK ((state='CLAIMED') = (claim_token IS NOT NULL AND lease_deadline IS NOT NULL))
);

CREATE TABLE memory_domain_bind_settlements (
    claim_token text PRIMARY KEY,
    input_payload jsonb NOT NULL,
    result_payload jsonb NOT NULL,
    settled_at timestamptz NOT NULL DEFAULT clock_timestamp()
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
        eligibility_state IN ('REGISTERED','ADMITTED','ELIGIBLE','SUPPRESSED')
    ),
    observed_at timestamptz NOT NULL,
    qualified_at timestamptz,
    PRIMARY KEY (bot_id,occurrence_id),
    UNIQUE (bot_id,binding_ref,event_ref),
    UNIQUE (bot_id,conversation_ref,event_ref),
    FOREIGN KEY (bot_id,source_unit_id) REFERENCES source_units(bot_id,source_unit_id),
    CHECK ((input_batch_ref IS NULL) = (context_revision IS NULL)),
    CHECK (
        (eligibility_state IN ('ADMITTED','ELIGIBLE') AND input_batch_ref IS NOT NULL)
        OR (eligibility_state IN ('REGISTERED','SUPPRESSED') AND input_batch_ref IS NULL)
    )
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
    resulting_eligibility_revision bigint NOT NULL CHECK (resulting_eligibility_revision >= 0),
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

CREATE TABLE unavailable_source_events (
    bot_id text NOT NULL REFERENCES forest_kernel.bot_identity(bot_id),
    conversation_ref text NOT NULL CHECK (octet_length(conversation_ref) BETWEEN 1 AND 256),
    event_ref text NOT NULL CHECK (octet_length(event_ref) BETWEEN 1 AND 256),
    operation_id text NOT NULL CHECK (octet_length(operation_id) BETWEEN 1 AND 256),
    input_digest text NOT NULL CHECK (input_digest ~ '^sha256:[a-f0-9]{64}$'),
    error_code text NOT NULL CHECK (error_code ~ '^[A-Z][A-Z0-9_]{0,127}$'),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (bot_id,conversation_ref,event_ref),
    UNIQUE (bot_id,operation_id)
);

CREATE TABLE pending_archive_events (
    bot_id text NOT NULL REFERENCES forest_kernel.bot_identity(bot_id),
    conversation_ref text NOT NULL CHECK (octet_length(conversation_ref) BETWEEN 1 AND 256),
    event_ref text NOT NULL CHECK (octet_length(event_ref) BETWEEN 1 AND 256),
    operation_id text NOT NULL CHECK (octet_length(operation_id) BETWEEN 1 AND 256),
    input_digest text NOT NULL CHECK (input_digest ~ '^sha256:[a-f0-9]{64}$'),
    compaction_ref text NOT NULL CHECK (octet_length(compaction_ref) BETWEEN 1 AND 256),
    input_batch_ref text NOT NULL CHECK (octet_length(input_batch_ref) BETWEEN 1 AND 256),
    context_revision bigint NOT NULL CHECK (context_revision > 0),
    accepted_deletion_epoch bigint NOT NULL CHECK (accepted_deletion_epoch >= 0),
    state text NOT NULL DEFAULT 'PENDING_CAPTURE' CHECK (
        state IN ('PENDING_CAPTURE','RESOLVED','UNAVAILABLE')
    ),
    source_unit_id text,
    occurrence_id text,
    error_code text,
    resolved_at timestamptz,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (bot_id,conversation_ref,event_ref),
    UNIQUE (bot_id,operation_id,event_ref),
    FOREIGN KEY (bot_id,source_unit_id) REFERENCES source_units(bot_id,source_unit_id),
    FOREIGN KEY (bot_id,occurrence_id) REFERENCES source_occurrences(bot_id,occurrence_id),
    CHECK (
        (state='PENDING_CAPTURE' AND source_unit_id IS NULL AND occurrence_id IS NULL
            AND error_code IS NULL AND resolved_at IS NULL)
        OR (state='RESOLVED' AND source_unit_id IS NOT NULL AND occurrence_id IS NOT NULL
            AND error_code IS NULL AND resolved_at IS NOT NULL)
        OR (state='UNAVAILABLE' AND source_unit_id IS NULL AND occurrence_id IS NULL
            AND error_code IS NOT NULL AND resolved_at IS NOT NULL)
    )
);

CREATE TABLE archive_qualification_operations (
    bot_id text NOT NULL,
    operation_id text NOT NULL CHECK (octet_length(operation_id) BETWEEN 1 AND 256),
    input_digest text NOT NULL CHECK (input_digest ~ '^sha256:[a-f0-9]{64}$'),
    compaction_ref text NOT NULL CHECK (octet_length(compaction_ref) BETWEEN 1 AND 256),
    conversation_ref text NOT NULL CHECK (octet_length(conversation_ref) BETWEEN 1 AND 256),
    event_ref text NOT NULL CHECK (octet_length(event_ref) BETWEEN 1 AND 256),
    source_unit_id text NOT NULL,
    occurrence_id text NOT NULL,
    resulting_eligibility_revision bigint NOT NULL CHECK (resulting_eligibility_revision > 0),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (bot_id,operation_id,event_ref),
    UNIQUE (bot_id,compaction_ref,event_ref),
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

CREATE TABLE source_forget_operations (
    bot_id text NOT NULL REFERENCES forest_kernel.bot_identity(bot_id),
    operation_id text NOT NULL CHECK (octet_length(operation_id) BETWEEN 1 AND 256),
    input_digest text NOT NULL CHECK (input_digest ~ '^sha256:[a-f0-9]{64}$'),
    source_unit_id text NOT NULL,
    selection_digest text NOT NULL CHECK (selection_digest ~ '^sha256:[a-f0-9]{64}$'),
    deletion_epoch bigint NOT NULL CHECK (deletion_epoch > 0),
    result_payload jsonb NOT NULL,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (bot_id,operation_id),
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
    batch_index integer NOT NULL CHECK (batch_index BETWEEN 0 AND 65535),
    facet_count integer NOT NULL CHECK (
        facet_count BETWEEN 0 AND 64 AND (target_kind='QUERY' OR facet_count>0)
    ),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (bot_id,operation_id),
    UNIQUE (bot_id,target_kind,target_id,batch_index)
);

CREATE TABLE query_facet_preparations (
    bot_id text NOT NULL,
    query_id text NOT NULL,
    facet_count integer NOT NULL CHECK (facet_count BETWEEN 0 AND 256),
    accepted_deletion_epoch bigint NOT NULL CHECK (accepted_deletion_epoch>=0),
    completed_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (bot_id,query_id),
    FOREIGN KEY (bot_id,query_id) REFERENCES memory_queries(bot_id,query_id)
);

CREATE TABLE facet_preparation_work (
    bot_id text NOT NULL,
    source_unit_id text NOT NULL,
    state text NOT NULL DEFAULT 'PENDING'
        CHECK (state IN ('PENDING','CLAIMED','READY','SUPPRESSED','FAILED')),
    claim_token text UNIQUE,
    lease_deadline timestamptz,
    attempt_count integer NOT NULL DEFAULT 0 CHECK (attempt_count >= 0),
    next_attempt_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    prepared_facet_count integer CHECK (
        prepared_facet_count IS NULL OR prepared_facet_count BETWEEN 0 AND 4096
    ),
    last_error_code text,
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (bot_id,source_unit_id),
    FOREIGN KEY (bot_id,source_unit_id) REFERENCES source_units(bot_id,source_unit_id),
    CHECK ((state='CLAIMED') = (claim_token IS NOT NULL AND lease_deadline IS NOT NULL)),
    CHECK ((state='READY') = (prepared_facet_count IS NOT NULL))
);

CREATE TABLE facet_preparation_settlements (
    bot_id text NOT NULL,
    claim_token text NOT NULL,
    input_payload jsonb NOT NULL,
    result_payload jsonb NOT NULL,
    settled_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (bot_id,claim_token)
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
    next_attempt_at timestamptz NOT NULL DEFAULT clock_timestamp(),
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

CREATE TABLE embedding_claims (
    bot_id text NOT NULL REFERENCES forest_kernel.bot_identity(bot_id),
    claim_id text NOT NULL CHECK (octet_length(claim_id) BETWEEN 1 AND 256),
    purpose text NOT NULL CHECK (purpose IN ('DOCUMENT','QUERY')),
    space_id text NOT NULL CHECK (octet_length(space_id) BETWEEN 1 AND 256),
    source_epoch_high_watermark bigint NOT NULL CHECK (source_epoch_high_watermark >= 0),
    lease_seconds integer NOT NULL CHECK (lease_seconds BETWEEN 3 AND 86400),
    lease_deadline timestamptz NOT NULL,
    item_limit integer NOT NULL CHECK (item_limit BETWEEN 1 AND 32),
    item_count integer NOT NULL CHECK (item_count BETWEEN 0 AND item_limit),
    claim_payload jsonb NOT NULL,
    state text NOT NULL DEFAULT 'CLAIMED' CHECK (state IN ('CLAIMED','EXPIRED','SETTLED')),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    settled_at timestamptz,
    PRIMARY KEY (bot_id,claim_id),
    CHECK ((state='SETTLED') = (settled_at IS NOT NULL))
);

CREATE TABLE embedding_claim_items (
    bot_id text NOT NULL,
    claim_id text NOT NULL,
    item_index integer NOT NULL CHECK (item_index BETWEEN 0 AND 31),
    facet_id text NOT NULL,
    PRIMARY KEY (bot_id,claim_id,item_index),
    UNIQUE (bot_id,claim_id,facet_id),
    FOREIGN KEY (bot_id,claim_id) REFERENCES embedding_claims(bot_id,claim_id),
    FOREIGN KEY (bot_id,facet_id) REFERENCES embedding_facets(bot_id,facet_id)
);

CREATE TABLE embedding_batch_attempts (
    bot_id text NOT NULL,
    attempt_id text NOT NULL CHECK (octet_length(attempt_id) BETWEEN 1 AND 256),
    claim_id text NOT NULL,
    effect_operation_id text NOT NULL CHECK (octet_length(effect_operation_id) BETWEEN 1 AND 256),
    executor_component_id text NOT NULL
        CHECK (octet_length(executor_component_id) BETWEEN 1 AND 128),
    executor_generation_id text NOT NULL
        CHECK (octet_length(executor_generation_id) BETWEEN 1 AND 256),
    fence_graph_id text NOT NULL CHECK (octet_length(fence_graph_id) BETWEEN 1 AND 256),
    fence_graph_revision bigint NOT NULL CHECK (fence_graph_revision > 0),
    accepted_deletion_epoch bigint NOT NULL CHECK (accepted_deletion_epoch >= 0),
    effect_wire_payload_id text NOT NULL
        CHECK (octet_length(effect_wire_payload_id) BETWEEN 1 AND 256),
    effect_wire_payload_digest text NOT NULL CHECK (
        effect_wire_payload_digest ~ '^sha256:[a-f0-9]{64}$'
    ),
    reservation_id text NOT NULL CHECK (octet_length(reservation_id) BETWEEN 1 AND 256),
    deadline_at timestamptz NOT NULL,
    state text NOT NULL DEFAULT 'PREPARED' CHECK (
        state IN ('PREPARED','DISPATCHED','SUCCEEDED','NOT_SENT','UNKNOWN','FAILED')
    ),
    handoff_operation_id text CHECK (
        handoff_operation_id IS NULL OR octet_length(handoff_operation_id) BETWEEN 1 AND 256
    ),
    settlement_operation_id text CHECK (
        settlement_operation_id IS NULL
        OR octet_length(settlement_operation_id) BETWEEN 1 AND 256
    ),
    settlement_input_digest text CHECK (
        settlement_input_digest IS NULL
        OR settlement_input_digest ~ '^sha256:[a-f0-9]{64}$'
    ),
    result_payload_id text CHECK (
        result_payload_id IS NULL OR octet_length(result_payload_id) BETWEEN 1 AND 256
    ),
    result_payload_digest text CHECK (
        result_payload_digest IS NULL OR result_payload_digest ~ '^sha256:[a-f0-9]{64}$'
    ),
    error_code text CHECK (error_code IS NULL OR octet_length(error_code) BETWEEN 1 AND 128),
    prepared_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    dispatched_at timestamptz,
    settled_at timestamptz,
    PRIMARY KEY (bot_id,attempt_id),
    UNIQUE (bot_id,claim_id),
    UNIQUE (bot_id,effect_operation_id),
    FOREIGN KEY (bot_id,claim_id) REFERENCES embedding_claims(bot_id,claim_id),
    CHECK (state<>'PREPARED' OR handoff_operation_id IS NULL),
    CHECK (state<>'PREPARED' OR dispatched_at IS NULL),
    CHECK (
        state NOT IN ('DISPATCHED','SUCCEEDED','UNKNOWN','FAILED')
        OR (handoff_operation_id IS NOT NULL AND dispatched_at IS NOT NULL)
    ),
    CHECK ((state IN ('PREPARED','DISPATCHED')) = (settlement_operation_id IS NULL)),
    CHECK ((state IN ('PREPARED','DISPATCHED')) = (settlement_input_digest IS NULL)),
    CHECK ((state IN ('PREPARED','DISPATCHED')) = (settled_at IS NULL)),
    CHECK ((state='SUCCEEDED') = (result_payload_id IS NOT NULL)),
    CHECK ((state='SUCCEEDED') = (result_payload_digest IS NOT NULL))
);

CREATE TABLE embedding_recovery_work (
    bot_id text NOT NULL,
    claim_id text NOT NULL,
    state text NOT NULL DEFAULT 'PENDING'
        CHECK (state IN ('PENDING','CLAIMED','SETTLED')),
    claim_token text CHECK (claim_token IS NULL OR octet_length(claim_token) BETWEEN 1 AND 256),
    claim_payload jsonb,
    lease_deadline timestamptz,
    attempt_count integer NOT NULL DEFAULT 0 CHECK (attempt_count >= 0),
    next_attempt_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (bot_id,claim_id),
    UNIQUE (bot_id,claim_token),
    FOREIGN KEY (bot_id,claim_id) REFERENCES embedding_claims(bot_id,claim_id),
    CHECK ((state='CLAIMED') =
        (claim_token IS NOT NULL AND claim_payload IS NOT NULL AND lease_deadline IS NOT NULL))
);

CREATE TABLE embedding_work_settlements (
    bot_id text NOT NULL,
    claim_token text NOT NULL,
    input_payload jsonb NOT NULL,
    result_payload jsonb NOT NULL,
    PRIMARY KEY (bot_id,claim_token)
);

ALTER TABLE embedding_facets ADD CONSTRAINT embedding_facet_current_attempt_fk
    FOREIGN KEY (bot_id,current_attempt_id)
    REFERENCES embedding_batch_attempts(bot_id,attempt_id);

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
    settlement_input_digest text CHECK (
        settlement_input_digest IS NULL OR settlement_input_digest ~ '^sha256:[a-f0-9]{64}$'
    ),
    charged_tokens bigint CHECK (charged_tokens IS NULL OR charged_tokens >= 0),
    error_code text CHECK (error_code IS NULL OR octet_length(error_code) BETWEEN 1 AND 128),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    settled_at timestamptz,
    PRIMARY KEY (bot_id,reservation_id),
    UNIQUE (bot_id,operation_id),
    UNIQUE (bot_id,batch_ref),
    UNIQUE (bot_id,reservation_id,batch_ref),
    UNIQUE (bot_id,settlement_operation_id),
    FOREIGN KEY (bot_id,budget_day) REFERENCES embedding_budget_days(bot_id,budget_day),
    CHECK ((state='RESERVED') = (settled_at IS NULL)),
    CHECK ((state='RESERVED') = (settlement_operation_id IS NULL)),
    CHECK ((state='RESERVED') = (settlement_input_digest IS NULL)),
    CHECK ((state='CHARGED') = (charged_tokens IS NOT NULL))
);

ALTER TABLE embedding_batch_attempts ADD CONSTRAINT embedding_batch_reservation_fk
    FOREIGN KEY (bot_id,reservation_id,claim_id)
    REFERENCES embedding_budget_reservations(bot_id,reservation_id,batch_ref);

CREATE TABLE retrieval_bundles (
    bot_id text NOT NULL,
    bundle_id text NOT NULL CHECK (octet_length(bundle_id) BETWEEN 1 AND 256),
    query_id text NOT NULL,
    operation_id text NOT NULL CHECK (octet_length(operation_id) BETWEEN 1 AND 256),
    input_digest text NOT NULL CHECK (input_digest ~ '^sha256:[a-f0-9]{64}$'),
    index_snapshot_at timestamptz NOT NULL,
    accepted_deletion_epoch bigint NOT NULL CHECK (accepted_deletion_epoch >= 0),
    result_limit integer NOT NULL CHECK (result_limit BETWEEN 1 AND 256),
    seed_limit integer NOT NULL CHECK (seed_limit BETWEEN 1 AND 256),
    candidate_limit_per_facet integer NOT NULL CHECK (
        candidate_limit_per_facet BETWEEN 1 AND 256
    ),
    min_similarity real NOT NULL CHECK (min_similarity BETWEEN -1 AND 1),
    min_graph_relevance real NOT NULL CHECK (
        min_graph_relevance > 0 AND min_graph_relevance <= 1
    ),
    sequence_weight real NOT NULL CHECK (sequence_weight > 0 AND sequence_weight <= 1),
    association_weight real NOT NULL CHECK (
        association_weight > 0 AND association_weight <= 1
    ),
    max_association_edges_per_node integer NOT NULL CHECK (
        max_association_edges_per_node BETWEEN 1 AND 4096
    ),
    sampling_seed text NOT NULL CHECK (octet_length(sampling_seed) BETWEEN 1 AND 256),
    seed_count integer NOT NULL CHECK (seed_count BETWEEN 0 AND seed_limit),
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
    activation_micros bigint NOT NULL CHECK (
        activation_micros > 0 AND activation_micros <= 1000000
    ),
    is_seed boolean NOT NULL,
    path_parent_source_unit_id text,
    path_edge_id text,
    PRIMARY KEY (bot_id,bundle_id,item_index),
    UNIQUE (bot_id,bundle_id,source_unit_id),
    FOREIGN KEY (bot_id,bundle_id) REFERENCES retrieval_bundles(bot_id,bundle_id),
    FOREIGN KEY (bot_id,source_unit_id) REFERENCES source_units(bot_id,source_unit_id),
    CHECK ((path_parent_source_unit_id IS NULL) = (path_edge_id IS NULL)),
    CHECK (is_seed OR path_parent_source_unit_id IS NOT NULL)
);

CREATE TABLE retrieval_projections (
    bot_id text NOT NULL,
    bundle_id text NOT NULL,
    projection_digest text NOT NULL CHECK (projection_digest ~ '^sha256:[a-f0-9]{64}$'),
    projected_count integer NOT NULL CHECK (projected_count BETWEEN 0 AND 256),
    projected_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (bot_id,bundle_id),
    FOREIGN KEY (bot_id,bundle_id) REFERENCES retrieval_bundles(bot_id,bundle_id)
);

CREATE TABLE retrieval_relations (
    bot_id text NOT NULL,
    bundle_id text NOT NULL,
    relation_index integer NOT NULL CHECK (relation_index BETWEEN 0 AND 65535),
    edge_id text NOT NULL CHECK (octet_length(edge_id) BETWEEN 1 AND 256),
    relation_kind text NOT NULL CHECK (relation_kind IN ('SEQUENCE','ASSOCIATED')),
    source_unit_id text NOT NULL,
    target_unit_id text NOT NULL,
    weight_micros bigint NOT NULL CHECK (weight_micros BETWEEN 1 AND 1000000),
    contribution_id text,
    PRIMARY KEY (bot_id,bundle_id,relation_index),
    UNIQUE (bot_id,bundle_id,edge_id,source_unit_id,target_unit_id),
    FOREIGN KEY (bot_id,bundle_id) REFERENCES retrieval_bundles(bot_id,bundle_id),
    FOREIGN KEY (bot_id,source_unit_id) REFERENCES source_units(bot_id,source_unit_id),
    FOREIGN KEY (bot_id,target_unit_id) REFERENCES source_units(bot_id,source_unit_id),
    CHECK (source_unit_id<>target_unit_id),
    CHECK ((relation_kind='ASSOCIATED') = (contribution_id IS NOT NULL))
);

CREATE TABLE association_journal (
    bot_id text NOT NULL,
    journal_id text NOT NULL CHECK (octet_length(journal_id) BETWEEN 1 AND 256),
    operation_id text NOT NULL CHECK (octet_length(operation_id) BETWEEN 1 AND 256),
    input_digest text NOT NULL CHECK (input_digest ~ '^sha256:[a-f0-9]{64}$'),
    reply_ref text NOT NULL CHECK (octet_length(reply_ref) BETWEEN 1 AND 512),
    bundle_id text NOT NULL,
    trigger_source_unit_id text NOT NULL,
    seed_source_unit_id text NOT NULL,
    seed_rank_micros bigint NOT NULL CHECK (seed_rank_micros >= 0),
    state text NOT NULL DEFAULT 'PENDING_ARCHIVE' CHECK (
        state IN ('PENDING_ARCHIVE','PROMOTED','SUPPRESSED')
    ),
    contribution_id text,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    promoted_at timestamptz,
    PRIMARY KEY (bot_id,journal_id),
    UNIQUE (bot_id,operation_id,bundle_id,trigger_source_unit_id,seed_source_unit_id),
    FOREIGN KEY (bot_id,bundle_id) REFERENCES retrieval_bundles(bot_id,bundle_id),
    FOREIGN KEY (bot_id,trigger_source_unit_id)
        REFERENCES source_units(bot_id,source_unit_id),
    FOREIGN KEY (bot_id,seed_source_unit_id)
        REFERENCES source_units(bot_id,source_unit_id),
    CHECK (trigger_source_unit_id<>seed_source_unit_id),
    CHECK ((state='PROMOTED') = (contribution_id IS NOT NULL)),
    CHECK ((state='PROMOTED') = (promoted_at IS NOT NULL))
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
    claim_token text CHECK (claim_token IS NULL OR octet_length(claim_token) BETWEEN 1 AND 256),
    claim_payload jsonb,
    lease_deadline timestamptz,
    attempt_count integer NOT NULL DEFAULT 0 CHECK (attempt_count >= 0),
    next_attempt_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    deleted_at timestamptz,
    PRIMARY KEY (bot_id,index_ref_id),
    UNIQUE (bot_id,claim_token),
    CHECK ((state='DELETED') = (deleted_at IS NOT NULL)),
    CHECK ((claim_token IS NULL) =
        (claim_payload IS NULL AND lease_deadline IS NULL))
);
CREATE TABLE vector_publish_batches (
    bot_id text NOT NULL,
    attempt_id text NOT NULL,
    state text NOT NULL DEFAULT 'PENDING' CHECK (state IN ('PENDING','CLAIMED','SETTLED')),
    claim_token text CHECK (claim_token IS NULL OR octet_length(claim_token) BETWEEN 1 AND 256),
    claim_payload jsonb,
    lease_deadline timestamptz,
    attempt_count integer NOT NULL DEFAULT 0 CHECK (attempt_count >= 0),
    next_attempt_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (bot_id,attempt_id),
    UNIQUE (bot_id,claim_token),
    FOREIGN KEY (bot_id,attempt_id) REFERENCES embedding_batch_attempts(bot_id,attempt_id),
    CHECK ((state='CLAIMED') =
        (claim_token IS NOT NULL AND claim_payload IS NOT NULL AND lease_deadline IS NOT NULL))
);
CREATE TABLE vector_publish_operations (
    bot_id text NOT NULL,
    operation_id text NOT NULL CHECK (octet_length(operation_id) BETWEEN 1 AND 256),
    input_digest text NOT NULL CHECK (input_digest ~ '^sha256:[a-f0-9]{64}$'),
    result_payload jsonb NOT NULL,
    PRIMARY KEY (bot_id,operation_id)
);
CREATE TABLE vector_work_settlements (
    bot_id text NOT NULL,
    claim_token text NOT NULL,
    input_payload jsonb NOT NULL,
    result_payload jsonb NOT NULL,
    PRIMARY KEY (bot_id,claim_token)
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
CREATE INDEX eligibility_operations_batch_idx ON eligibility_operations(
    bot_id,input_batch_ref,context_revision,occurrence_id
);
CREATE INDEX source_occurrences_unit_idx ON source_occurrences(
    bot_id,source_unit_id,conversation_ref,ingress_seq
) INCLUDE (eligibility_state,input_batch_ref,context_revision);
CREATE INDEX pending_input_events_batch_idx ON pending_input_events(
    bot_id,input_batch_ref,context_revision,state,conversation_ref
);
CREATE INDEX relation_left_idx ON relation_contributions(
    bot_id,left_kind,left_id,active,relation_id,contribution_id
);
CREATE INDEX relation_right_idx ON relation_contributions(
    bot_id,right_kind,right_id,active,relation_id,contribution_id
);
CREATE INDEX embedding_facets_work_idx ON embedding_facets(
    bot_id,state,purpose,next_attempt_at,created_at,facet_id
);
CREATE INDEX facet_preparation_work_idx ON facet_preparation_work(
    bot_id,state,next_attempt_at,source_unit_id
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
CREATE INDEX embedding_batch_attempts_recovery_idx ON embedding_batch_attempts(
    bot_id,state,deadline_at,attempt_id
);
CREATE INDEX embedding_recovery_work_poll_idx ON embedding_recovery_work(
    bot_id,state,next_attempt_at,claim_id
);
CREATE INDEX vector_index_deletions_work_idx ON vector_index_deletions(
    bot_id,state,next_attempt_at,index_ref_id
);
CREATE INDEX vector_publish_batches_poll_idx ON vector_publish_batches(
    bot_id,state,next_attempt_at,attempt_id
);
CREATE INDEX embedding_claims_created_idx ON embedding_claims(
    bot_id,purpose,created_at,claim_id
);
CREATE INDEX embedding_claims_active_idx ON embedding_claims(
    bot_id,state,lease_deadline,claim_id
);
CREATE INDEX embedding_claim_items_facet_idx ON embedding_claim_items(
    bot_id,facet_id,claim_id
);
CREATE INDEX embedding_budget_reservations_state_idx ON embedding_budget_reservations(
    bot_id,state,budget_day,reservation_id
);

CREATE FUNCTION reject_immutable_row() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    RAISE EXCEPTION 'memory.graph immutable fact cannot change' USING ERRCODE='55000';
END;
$$;

CREATE FUNCTION memory_begin_query(context jsonb, envelope jsonb)
RETURNS jsonb LANGUAGE plpgsql VOLATILE SET search_path = pg_catalog AS $$
DECLARE
    body jsonb := envelope->'input';
    requested_bot text := context->>'bot_id';
    requested_query text := body->>'query_id';
    requested_operation text := context->>'operation_id';
    requested_digest text := context->>'participant_input_digest';
    epoch_value jsonb := envelope->'prior'->(body->>'deletion_epoch_step_id');
    domain_row __forest_module_schema__.memory_domains%ROWTYPE;
    query_row __forest_module_schema__.memory_queries%ROWTYPE;
    selected_parts jsonb;
    selected_source text;
    source_count integer;
    text_count integer;
    image_count integer;
BEGIN
    IF envelope->>'protocol' IS DISTINCT FROM 'forest.uow-step/1'
       OR envelope#>>'{caller,component_id}' IS DISTINCT FROM 'memory.graph'
       OR body->>'protocol' IS DISTINCT FROM 'forest.memory-query-begin/1'
       OR epoch_value->>'protocol' IS DISTINCT FROM 'forest.payload-epoch/1'
       OR epoch_value->>'deletion_epoch' IS NULL THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object('code','MEMORY_QUERY_INPUT_INVALID',
                'message','Query preparation requires a current Payload epoch','status',422));
    END IF;
    PERFORM pg_advisory_xact_lock(hashtextextended(
        'forest.memory.graph/query/'||requested_bot||'/'||requested_query,0));
    SELECT * INTO domain_row FROM __forest_module_schema__.memory_domains
    WHERE bot_id=requested_bot FOR SHARE;
    IF NOT FOUND THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object('code','MEMORY_DOMAIN_UNBOUND',
                'message','Query requires a bound embedding domain','status',409));
    END IF;
    SELECT * INTO query_row FROM __forest_module_schema__.memory_queries
    WHERE bot_id=requested_bot AND query_id=requested_query;
    IF FOUND THEN
        IF query_row.operation_id IS DISTINCT FROM requested_operation
           OR query_row.input_digest IS DISTINCT FROM requested_digest THEN
            RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
                'error',jsonb_build_object('code','MEMORY_QUERY_IDENTITY_CONFLICT',
                    'message','Query identity was bound to a different input','status',409));
        END IF;
    ELSE
        IF EXISTS (
            SELECT 1 FROM __forest_module_schema__.pending_input_events
            WHERE bot_id=requested_bot AND conversation_ref=body->>'conversation_ref'
              AND input_batch_ref=body->>'input_batch_ref'
              AND context_revision=(body->>'context_revision')::bigint
              AND state='PENDING_CAPTURE'
        ) THEN
            RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
                'error',jsonb_build_object('code','MEMORY_QUERY_CAPTURE_PENDING',
                    'message','Query input still has unresolved source capture','status',409));
        END IF;
        PERFORM o.occurrence_id
        FROM __forest_module_schema__.eligibility_operations e
        JOIN __forest_module_schema__.source_occurrences o USING(bot_id,occurrence_id,source_unit_id)
        JOIN __forest_module_schema__.source_units u USING(bot_id,source_unit_id)
        JOIN __forest_module_schema__.memory_sources s USING(bot_id,source_id)
        WHERE o.bot_id=requested_bot AND o.conversation_ref=body->>'conversation_ref'
          AND e.input_batch_ref=body->>'input_batch_ref'
          AND e.context_revision=(body->>'context_revision')::bigint
        ORDER BY u.source_unit_id,o.occurrence_id FOR SHARE OF o,u,s;
        IF EXISTS (
            SELECT 1 FROM __forest_module_schema__.eligibility_operations e
            JOIN __forest_module_schema__.source_occurrences o USING(bot_id,occurrence_id,source_unit_id)
            JOIN __forest_module_schema__.source_units u USING(bot_id,source_unit_id)
            JOIN __forest_module_schema__.memory_sources s USING(bot_id,source_id)
            WHERE e.bot_id=requested_bot AND o.conversation_ref=body->>'conversation_ref'
              AND e.input_batch_ref=body->>'input_batch_ref'
              AND e.context_revision=(body->>'context_revision')::bigint
              AND (o.eligibility_state NOT IN ('ADMITTED','ELIGIBLE')
                   OR u.eligibility_state NOT IN ('REGISTERED','ELIGIBLE') OR s.status<>'ACTIVE'
                   OR o.input_batch_ref IS DISTINCT FROM e.input_batch_ref
                   OR o.context_revision IS DISTINCT FROM e.context_revision)
        ) THEN
            RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
                'error',jsonb_build_object('code','MEMORY_QUERY_INPUT_UNAVAILABLE',
                    'message','Admitted query membership was revoked','status',409));
        END IF;
        WITH members AS (
            SELECT DISTINCT ON (u.source_unit_id)
                u.source_id,u.source_unit_id,u.source_revision,u.accepted_deletion_epoch,
                o.occurrence_id,o.ingress_seq
            FROM __forest_module_schema__.eligibility_operations e
            JOIN __forest_module_schema__.source_occurrences o
              USING(bot_id,occurrence_id,source_unit_id)
            JOIN __forest_module_schema__.source_units u USING(bot_id,source_unit_id)
            JOIN __forest_module_schema__.memory_sources s USING(bot_id,source_id)
            WHERE o.bot_id=requested_bot AND o.conversation_ref=body->>'conversation_ref'
              AND e.input_batch_ref=body->>'input_batch_ref'
              AND e.context_revision=(body->>'context_revision')::bigint
              AND o.eligibility_state IN ('ADMITTED','ELIGIBLE')
              AND u.eligibility_state IN ('REGISTERED','ELIGIBLE') AND s.status='ACTIVE'
              AND u.accepted_deletion_epoch<=(epoch_value->>'deletion_epoch')::bigint
            ORDER BY u.source_unit_id,o.ingress_seq,o.occurrence_id
        ), parts AS (
            SELECT m.*,p.part_id,p.part_index,p.part_kind
            FROM members m JOIN __forest_module_schema__.source_parts p
              ON p.bot_id=requested_bot AND p.source_unit_id=m.source_unit_id
            ORDER BY m.ingress_seq,m.occurrence_id,p.part_index LIMIT 257
        )
        SELECT jsonb_agg(to_jsonb(parts) ORDER BY ingress_seq,occurrence_id,part_index),
               min(source_id),count(DISTINCT source_id),
               count(*) FILTER (WHERE part_kind='TEXT'),
               count(*) FILTER (WHERE part_kind='IMAGE')
        INTO selected_parts,selected_source,source_count,text_count,image_count FROM parts;
        IF selected_parts IS NULL OR source_count<>1 THEN
            RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
                'error',jsonb_build_object('code','MEMORY_QUERY_INPUT_UNAVAILABLE',
                    'message','Query does not identify one admitted conversation source','status',409));
        END IF;
        IF jsonb_array_length(selected_parts)>256 THEN
            RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
                'error',jsonb_build_object('code','MEMORY_QUERY_PART_LIMIT',
                    'message','Query input exceeds 256 source parts','status',422));
        END IF;
        INSERT INTO __forest_module_schema__.memory_queries(
            bot_id,query_id,operation_id,input_digest,current_source_id,input_batch_ref,
            context_revision,query_mode,fusion_policy_revision,accepted_deletion_epoch)
        VALUES(requested_bot,requested_query,requested_operation,requested_digest,
            selected_source,body->>'input_batch_ref',(body->>'context_revision')::bigint,
            CASE WHEN text_count=0 THEN 'IMAGE' WHEN image_count=0 THEN 'TEXT' ELSE 'MIXED' END,
            domain_row.fusion_policy_revision,(epoch_value->>'deletion_epoch')::bigint)
        RETURNING * INTO query_row;
        INSERT INTO __forest_module_schema__.memory_query_parts(
            bot_id,query_id,query_part_index,source_unit_id,source_revision,
            occurrence_id,part_id,modality)
        SELECT requested_bot,requested_query,(ordinality-1)::integer,
               value->>'source_unit_id',(value->>'source_revision')::bigint,
               value->>'occurrence_id',value->>'part_id',value->>'part_kind'
        FROM jsonb_array_elements(selected_parts) WITH ORDINALITY;
    END IF;
    RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',true,
        'value',jsonb_build_object('protocol','forest.memory-query/1',
            'query_id',query_row.query_id,'space_id',domain_row.space_id,
            'input_batch_ref',query_row.input_batch_ref,'context_revision',query_row.context_revision,
            'query_mode',query_row.query_mode,'part_count',(
                SELECT count(*) FROM __forest_module_schema__.memory_query_parts
                WHERE bot_id=requested_bot AND query_id=requested_query)));
END;
$$;

CREATE FUNCTION memory_read_query_parts(context jsonb, input jsonb)
RETURNS jsonb LANGUAGE plpgsql STABLE SET search_path = pg_catalog AS $$
DECLARE
    requested_bot text := context->>'bot_id';
    requested_query text := input->>'query_id';
    query_row __forest_module_schema__.memory_queries%ROWTYPE;
    page_value jsonb;
    last_index integer;
    has_more boolean;
BEGIN
    SELECT * INTO query_row FROM __forest_module_schema__.memory_queries
    WHERE bot_id=requested_bot AND query_id=requested_query AND state='OPEN';
    IF NOT FOUND OR EXISTS (
        SELECT 1 FROM __forest_module_schema__.memory_query_parts qp
        JOIN __forest_module_schema__.source_units u USING(bot_id,source_unit_id)
        JOIN __forest_module_schema__.memory_sources s USING(bot_id,source_id)
        JOIN __forest_module_schema__.source_occurrences o
          ON o.bot_id=qp.bot_id AND o.occurrence_id=qp.occurrence_id
        WHERE qp.bot_id=requested_bot AND qp.query_id=requested_query
          AND (u.source_revision<>qp.source_revision
               OR u.eligibility_state NOT IN ('REGISTERED','ELIGIBLE') OR s.status<>'ACTIVE'
               OR o.eligibility_state NOT IN ('ADMITTED','ELIGIBLE')
               OR o.input_batch_ref IS DISTINCT FROM query_row.input_batch_ref
               OR o.context_revision IS DISTINCT FROM query_row.context_revision)
    ) THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object('code','MEMORY_QUERY_INPUT_UNAVAILABLE',
                'message','Frozen query source membership is unavailable','status',409));
    END IF;
    SELECT COALESCE(jsonb_agg(item ORDER BY query_part_index),'[]'::jsonb),max(query_part_index)
    INTO page_value,last_index FROM (
        SELECT qp.query_part_index,jsonb_build_object(
            'query_part_index',qp.query_part_index,'source_unit_id',qp.source_unit_id,
            'source_revision',qp.source_revision,'occurrence_id',qp.occurrence_id,
            'part_id',p.part_id,'part_index',p.part_index,'part_kind',p.part_kind,
            'media_type',p.media_type,'byte_length',p.byte_length,
            'payload',jsonb_build_object('payload_id',p.payload_id,'digest',p.payload_digest,
                'owner_component_id',p.payload_owner_component_id,
                'owner_kind',p.payload_owner_kind,'owner_id',p.payload_owner_id)) AS item
        FROM __forest_module_schema__.memory_query_parts qp
        JOIN __forest_module_schema__.source_parts p USING(bot_id,source_unit_id,part_id)
        WHERE qp.bot_id=requested_bot AND qp.query_id=requested_query
          AND qp.query_part_index>(input->>'after_part_index')::integer
        ORDER BY qp.query_part_index LIMIT (input->>'limit')::integer
    ) page;
    SELECT EXISTS (
        SELECT 1 FROM __forest_module_schema__.memory_query_parts
        WHERE bot_id=requested_bot AND query_id=requested_query AND query_part_index>last_index
    ) INTO has_more;
    RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',true,
        'value',jsonb_build_object('parts',page_value,'next_part_index',last_index,'has_more',has_more,
            'space_id',(SELECT space_id FROM __forest_module_schema__.memory_domains
                WHERE bot_id=requested_bot)));
END;
$$;

CREATE FUNCTION memory_read_query_vectors(context jsonb, input jsonb)
RETURNS jsonb LANGUAGE plpgsql STABLE SET search_path = pg_catalog AS $$
DECLARE
    requested_bot text := context->>'bot_id';
    requested_query text := input->>'query_id';
    page_check jsonb;
    facets_value jsonb;
    last_id text;
    has_more boolean;
    prepared_row __forest_module_schema__.query_facet_preparations%ROWTYPE;
BEGIN
    page_check := __forest_module_schema__.memory_read_query_parts(context,
        jsonb_build_object('query_id',requested_query,'after_part_index',-1,'limit',1));
    IF page_check->>'ok' IS DISTINCT FROM 'true' THEN RETURN page_check; END IF;
    SELECT * INTO prepared_row FROM __forest_module_schema__.query_facet_preparations
    WHERE bot_id=requested_bot AND query_id=requested_query;
    IF NOT FOUND OR prepared_row.facet_count<>(
        SELECT count(*) FROM __forest_module_schema__.embedding_facets
        WHERE bot_id=requested_bot AND query_id=requested_query AND purpose='QUERY' AND state='READY'
    ) THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object('code','MEMORY_QUERY_VECTORS_PENDING',
                'message','Query vectors are not all published','status',409));
    END IF;
    SELECT COALESCE(jsonb_agg(item ORDER BY facet_id),'[]'::jsonb),max(facet_id)
    INTO facets_value,last_id FROM (
        SELECT facet_id,jsonb_build_object('ref_id',index_ref_id,'modality',modality) AS item
        FROM __forest_module_schema__.embedding_facets
        WHERE bot_id=requested_bot AND query_id=requested_query AND purpose='QUERY' AND state='READY'
          AND (input->>'after_facet_id' IS NULL OR facet_id>input->>'after_facet_id')
        ORDER BY facet_id LIMIT (input->>'limit')::integer
    ) page;
    SELECT EXISTS (
        SELECT 1 FROM __forest_module_schema__.embedding_facets
        WHERE bot_id=requested_bot AND query_id=requested_query AND purpose='QUERY' AND facet_id>last_id
    ) INTO has_more;
    RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',true,
        'value',jsonb_build_object('space_id',page_check#>>'{value,space_id}',
            'index_snapshot_at',(SELECT created_at FROM __forest_module_schema__.memory_queries
                WHERE bot_id=requested_bot AND query_id=requested_query),
            'facets',facets_value,'next_facet_id',last_id,'has_more',has_more));
END;
$$;

CREATE FUNCTION memory_qualify_vector_candidates(context jsonb, input jsonb)
RETURNS jsonb LANGUAGE plpgsql STABLE SET search_path = pg_catalog AS $$
DECLARE
    requested_bot text := context->>'bot_id';
    requested_query text := input->>'query_id';
    query_row __forest_module_schema__.memory_queries%ROWTYPE;
    page_check jsonb;
    candidates_value jsonb;
    conversation_value text;
    cut_value bigint;
BEGIN
    page_check := __forest_module_schema__.memory_read_query_vectors(context,
        jsonb_build_object('query_id',requested_query,'after_facet_id',NULL,'limit',1));
    IF page_check->>'ok' IS DISTINCT FROM 'true' THEN RETURN page_check; END IF;
    SELECT * INTO query_row FROM __forest_module_schema__.memory_queries
    WHERE bot_id=requested_bot AND query_id=requested_query;
    IF NOT EXISTS (
        SELECT 1 FROM __forest_module_schema__.embedding_facets
        WHERE bot_id=requested_bot AND query_id=requested_query AND purpose='QUERY'
          AND index_ref_id=input->>'query_facet_id' AND state='READY'
    ) THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object('code','MEMORY_QUERY_FACET_MISMATCH',
                'message','Candidate qualification refers to another query facet','status',409));
    END IF;
    SELECT min(o.conversation_ref),max(o.ingress_seq) INTO conversation_value,cut_value
    FROM __forest_module_schema__.memory_query_parts qp
    JOIN __forest_module_schema__.source_occurrences o
      ON o.bot_id=qp.bot_id AND o.occurrence_id=qp.occurrence_id
    WHERE qp.bot_id=requested_bot AND qp.query_id=requested_query;
    SELECT COALESCE(jsonb_agg(jsonb_build_object(
        'ref_id',f.index_ref_id,'candidate_rank',(hit->>'candidate_rank')::integer,
        'score',(hit->>'score')::double precision,'source_unit_id',u.source_unit_id,
        'source_revision',u.source_revision,'source_id',s.source_id,'trunk_id',s.trunk_id,
        'source_kind',s.source_kind,'platform',s.platform,'realm_id',s.realm_id,
        'external_group_id',s.external_group_id,'author_subject_ref',u.author_subject_ref
    ) ORDER BY (hit->>'candidate_rank')::integer),'[]'::jsonb)
    INTO candidates_value
    FROM jsonb_array_elements(input->'candidates') hit
    JOIN __forest_module_schema__.embedding_facets f
      ON f.bot_id=requested_bot AND f.index_ref_id=hit->>'ref_id'
     AND f.purpose='DOCUMENT' AND f.state='READY'
     AND f.space_id=page_check#>>'{value,space_id}' AND f.published_at<=query_row.created_at
    JOIN __forest_module_schema__.source_units u
      ON u.bot_id=f.bot_id AND u.source_unit_id=f.source_unit_id
     AND u.source_revision=f.source_revision AND u.eligibility_state='ELIGIBLE'
    JOIN __forest_module_schema__.memory_sources s
      ON s.bot_id=u.bot_id AND s.source_id=u.source_id AND s.status='ACTIVE'
    WHERE EXISTS (
        SELECT 1 FROM __forest_module_schema__.source_occurrences o
        WHERE o.bot_id=u.bot_id AND o.source_unit_id=u.source_unit_id AND o.eligibility_state='ELIGIBLE'
    ) AND NOT EXISTS (
        SELECT 1 FROM __forest_module_schema__.memory_query_parts qp
        WHERE qp.bot_id=u.bot_id AND qp.query_id=requested_query AND qp.source_unit_id=u.source_unit_id
    ) AND NOT EXISTS (
        SELECT 1 FROM __forest_module_schema__.source_occurrences o
        WHERE o.bot_id=u.bot_id AND o.source_unit_id=u.source_unit_id
          AND o.conversation_ref=conversation_value
          AND (cut_value IS NULL OR o.ingress_seq IS NULL OR o.ingress_seq>cut_value
               OR o.eligibility_state='REGISTERED')
    );
    RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',true,
        'value',jsonb_build_object('candidates',candidates_value));
END;
$$;

CREATE FUNCTION memory_query_occurrence_matches(
    requested_bot text, requested_query text, requested_unit text,
    requested_revision bigint, requested_part text, requested_occurrence text
) RETURNS boolean LANGUAGE sql STABLE SET search_path = pg_catalog AS $$
    SELECT EXISTS (
        SELECT 1 FROM __forest_module_schema__.memory_query_parts p
        JOIN __forest_module_schema__.memory_queries q
          ON q.bot_id=p.bot_id AND q.query_id=p.query_id AND q.state='OPEN'
        JOIN __forest_module_schema__.query_facet_preparations prepared
          ON prepared.bot_id=q.bot_id AND prepared.query_id=q.query_id
        JOIN __forest_module_schema__.source_occurrences o
          ON o.bot_id=p.bot_id AND o.occurrence_id=p.occurrence_id
         AND o.source_unit_id=p.source_unit_id
         AND o.input_batch_ref=q.input_batch_ref AND o.context_revision=q.context_revision
         AND o.eligibility_state IN ('ADMITTED','ELIGIBLE')
        WHERE p.bot_id=requested_bot AND p.query_id=requested_query
          AND p.source_unit_id=requested_unit AND p.source_revision=requested_revision
          AND p.part_id=requested_part AND p.occurrence_id=requested_occurrence
    );
$$;

CREATE FUNCTION memory_claim_vector_publish(context jsonb, input jsonb)
RETURNS jsonb LANGUAGE plpgsql VOLATILE SET search_path = pg_catalog AS $$
DECLARE
    requested_bot text := context->>'bot_id';
    requested_token text := input->>'claim_token';
    requested_lease integer := (input->>'lease_seconds')::integer;
    work_row __forest_module_schema__.vector_publish_batches%ROWTYPE;
    attempt_row __forest_module_schema__.embedding_batch_attempts%ROWTYPE;
    claim_row __forest_module_schema__.embedding_claims%ROWTYPE;
    facets_value jsonb;
    task_payload jsonb;
BEGIN
    IF input->>'protocol' IS DISTINCT FROM 'forest.owned-work-claim-request/1'
       OR requested_bot IS NULL OR requested_token IS NULL
       OR octet_length(requested_token) NOT BETWEEN 1 AND 256
       OR requested_lease IS NULL OR requested_lease NOT BETWEEN 3 AND 86400 THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object('code','MEMORY_VECTOR_PUBLISH_CLAIM_INVALID',
                'message','Vector publish claim is invalid','status',422));
    END IF;
    SELECT * INTO work_row FROM __forest_module_schema__.vector_publish_batches
    WHERE bot_id=requested_bot AND claim_token=requested_token;
    IF FOUND THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',true,
            'value',work_row.claim_payload);
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM __forest_module_schema__.memory_domain_bind_work
        WHERE bot_id=requested_bot AND state='READY'
    ) THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',true,'value',NULL);
    END IF;
    INSERT INTO __forest_module_schema__.vector_publish_batches(bot_id,attempt_id)
    SELECT DISTINCT a.bot_id,a.attempt_id
    FROM __forest_module_schema__.embedding_batch_attempts a
    JOIN __forest_module_schema__.embedding_claim_items ci
      ON ci.bot_id=a.bot_id AND ci.claim_id=a.claim_id
    JOIN __forest_module_schema__.embedding_facets f
      ON f.bot_id=ci.bot_id AND f.facet_id=ci.facet_id
    WHERE a.bot_id=requested_bot AND a.state='SUCCEEDED'
      AND f.state='EMBEDDED'
    ORDER BY a.bot_id,a.attempt_id LIMIT 32
    ON CONFLICT (bot_id,attempt_id) DO NOTHING;
    SELECT w.* INTO work_row FROM __forest_module_schema__.vector_publish_batches w
    WHERE w.bot_id=requested_bot AND w.next_attempt_at<=clock_timestamp()
      AND (w.state='PENDING'
           OR (w.state='CLAIMED' AND w.lease_deadline<=clock_timestamp()))
    ORDER BY w.next_attempt_at,w.attempt_id LIMIT 1 FOR UPDATE SKIP LOCKED;
    IF NOT FOUND THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',true,'value',NULL);
    END IF;
    SELECT * INTO attempt_row FROM __forest_module_schema__.embedding_batch_attempts
    WHERE bot_id=requested_bot AND attempt_id=work_row.attempt_id;
    SELECT * INTO claim_row FROM __forest_module_schema__.embedding_claims
    WHERE bot_id=requested_bot AND claim_id=attempt_row.claim_id;
    IF attempt_row.state<>'SUCCEEDED' THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object('code','MEMORY_VECTOR_PUBLISH_ATTEMPT_INVALID',
                'message','Vector publish attempt is not settled','status',409));
    END IF;
    SELECT jsonb_agg(jsonb_build_object(
        'facet_id',f.facet_id,'item_index',ci.item_index,
        'source_unit_id',f.source_unit_id,'source_revision',f.source_revision,
        'modality',f.modality,'state',f.state,'vector_digest',f.vector_digest
    ) ORDER BY ci.item_index) INTO facets_value
    FROM __forest_module_schema__.embedding_claim_items ci
    JOIN __forest_module_schema__.embedding_facets f
      ON f.bot_id=ci.bot_id AND f.facet_id=ci.facet_id
    WHERE ci.bot_id=requested_bot AND ci.claim_id=attempt_row.claim_id;
    IF jsonb_array_length(facets_value)<>claim_row.item_count THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object('code','MEMORY_VECTOR_PUBLISH_MEMBERS_INVALID',
                'message','Vector publish batch membership changed','status',409));
    END IF;
    task_payload := jsonb_build_object(
        'protocol','forest.owned-work-claim/1',
        'work_id',work_row.attempt_id,'claim_token',requested_token,
        'attempt',work_row.attempt_count+1,
        'operation_id','memory-vector-publish:'||encode(sha256(convert_to(
            requested_bot||'/'||requested_token,'UTF8')),'hex'),
        'causal_root_kind','memory.embedding.batch',
        'causal_root_id',attempt_row.claim_id,
        'payload',jsonb_build_object(
            'attempt_id',work_row.attempt_id,'claim_token',requested_token,
            'space_id',claim_row.space_id,'purpose',claim_row.purpose,
            'result',jsonb_build_object(
                'payload_id',attempt_row.result_payload_id,
                'digest',attempt_row.result_payload_digest,
                'owner_component_id','embedding.multimodal',
                'owner_kind','embedding.result',
                'owner_id',attempt_row.attempt_id),
            'facets',facets_value));
    UPDATE __forest_module_schema__.vector_publish_batches
    SET state='CLAIMED',claim_token=requested_token,claim_payload=task_payload,
        lease_deadline=clock_timestamp()+(requested_lease*interval '1 second'),
        attempt_count=attempt_count+1,updated_at=clock_timestamp()
    WHERE bot_id=requested_bot AND attempt_id=work_row.attempt_id;
    RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',true,
        'value',task_payload);
EXCEPTION WHEN invalid_text_representation OR numeric_value_out_of_range THEN
    RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
        'error',jsonb_build_object('code','MEMORY_VECTOR_PUBLISH_CLAIM_INVALID',
            'message','Vector publish claim is invalid','status',422));
END;
$$;

CREATE FUNCTION memory_publish_vector_batch(context jsonb, envelope jsonb)
RETURNS jsonb LANGUAGE plpgsql VOLATILE SET search_path = pg_catalog AS $$
DECLARE
    body jsonb := envelope->'input';
    results_value jsonb := body->'results';
    epoch_value jsonb := envelope#>'{prior,payload_epoch}';
    requested_bot text := context->>'bot_id';
    requested_operation text := context->>'operation_id';
    requested_digest text := context->>'participant_input_digest';
    epoch_number bigint := (epoch_value->>'deletion_epoch')::bigint;
    work_row __forest_module_schema__.vector_publish_batches%ROWTYPE;
    attempt_row __forest_module_schema__.embedding_batch_attempts%ROWTYPE;
    claim_row __forest_module_schema__.embedding_claims%ROWTYPE;
    prior_row __forest_module_schema__.vector_publish_operations%ROWTYPE;
    facet_row record;
    item_result jsonb;
    source_eligible boolean;
    ready_count integer := 0;
    deletion_count integer := 0;
    seen_count integer := 0;
    result_value jsonb;
BEGIN
    IF envelope->>'protocol' IS DISTINCT FROM 'forest.uow-step/1'
       OR envelope#>>'{caller,component_id}' IS DISTINCT FROM 'memory.graph'
       OR body->>'protocol' IS DISTINCT FROM 'forest.memory-vector-publish/1'
       OR epoch_value->>'protocol' IS DISTINCT FROM 'forest.payload-epoch/1'
       OR requested_bot IS NULL OR requested_operation IS NULL
       OR requested_digest !~ '^sha256:[a-f0-9]{64}$'
       OR epoch_number IS NULL OR epoch_number<0
       OR jsonb_typeof(results_value) IS DISTINCT FROM 'array'
       OR body->>'attempt_id' IS NULL OR body->>'claim_token' IS NULL THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object('code','MEMORY_VECTOR_PUBLISH_INVALID',
                'message','Vector publication evidence is invalid','status',422));
    END IF;
    SELECT * INTO prior_row FROM __forest_module_schema__.vector_publish_operations
    WHERE bot_id=requested_bot AND operation_id=requested_operation;
    IF FOUND THEN
        IF prior_row.input_digest=requested_digest THEN
            RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',true,
                'value',prior_row.result_payload);
        END IF;
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object('code','MEMORY_VECTOR_PUBLISH_CONFLICT',
                'message','Vector publication operation was reused','status',409));
    END IF;
    SELECT * INTO work_row FROM __forest_module_schema__.vector_publish_batches
    WHERE bot_id=requested_bot AND attempt_id=body->>'attempt_id' FOR UPDATE;
    SELECT * INTO attempt_row FROM __forest_module_schema__.embedding_batch_attempts
    WHERE bot_id=requested_bot AND attempt_id=body->>'attempt_id';
    IF work_row IS NULL OR work_row.state<>'CLAIMED'
       OR work_row.claim_token IS DISTINCT FROM body->>'claim_token'
       OR attempt_row IS NULL OR attempt_row.state<>'SUCCEEDED' THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object('code','MEMORY_VECTOR_PUBLISH_CLAIM_LOST',
                'message','Vector publication claim is no longer current','status',409));
    END IF;
    SELECT * INTO claim_row FROM __forest_module_schema__.embedding_claims
    WHERE bot_id=requested_bot AND claim_id=attempt_row.claim_id;
    IF claim_row.space_id IS DISTINCT FROM
        body->>'space_id' OR jsonb_array_length(results_value)<>claim_row.item_count THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object('code','MEMORY_VECTOR_PUBLISH_MEMBERS_INVALID',
                'message','Vector publication membership differs','status',409));
    END IF;
    FOR facet_row IN
        SELECT ci.item_index,f.facet_id,f.state AS facet_state,f.source_unit_id,f.purpose,
               f.source_revision,f.space_id,f.accepted_deletion_epoch,
               f.index_ref_id,u.source_revision AS unit_revision,
               u.eligibility_state AS unit_state,
               u.accepted_deletion_epoch AS unit_epoch,
               s.status AS source_state,o.occurrence_id
        FROM __forest_module_schema__.embedding_claim_items ci
        JOIN __forest_module_schema__.embedding_facets f
          ON f.bot_id=ci.bot_id AND f.facet_id=ci.facet_id
        JOIN __forest_module_schema__.source_units u
          ON u.bot_id=f.bot_id AND u.source_unit_id=f.source_unit_id
        JOIN __forest_module_schema__.memory_sources s
          ON s.bot_id=u.bot_id AND s.source_id=u.source_id
        LEFT JOIN LATERAL (
            SELECT occurrence.occurrence_id
            FROM __forest_module_schema__.source_occurrences occurrence
            WHERE occurrence.bot_id=f.bot_id
              AND occurrence.source_unit_id=f.source_unit_id
              AND ((f.purpose='DOCUMENT' AND occurrence.eligibility_state='ELIGIBLE')
                   OR (f.purpose='QUERY' AND
                       __forest_module_schema__.memory_query_occurrence_matches(
                           f.bot_id,f.query_id,f.source_unit_id,f.source_revision,
                           f.part_id,occurrence.occurrence_id)))
            ORDER BY occurrence.occurrence_id LIMIT 1 FOR SHARE
        ) o ON true
        WHERE ci.bot_id=requested_bot AND ci.claim_id=attempt_row.claim_id
        ORDER BY ci.item_index FOR UPDATE OF f FOR SHARE OF u,s
    LOOP
        seen_count := seen_count+1;
        item_result := results_value->facet_row.item_index;
        IF item_result->>'facet_id' IS DISTINCT FROM facet_row.facet_id
           OR item_result->>'status' NOT IN ('PUBLISHED','TOMBSTONED','SKIPPED')
           OR facet_row.space_id IS DISTINCT FROM claim_row.space_id
           OR facet_row.purpose IS DISTINCT FROM claim_row.purpose
           OR facet_row.facet_state NOT IN ('EMBEDDED','READY','SUPPRESSED')
           OR facet_row.accepted_deletion_epoch>epoch_number
           OR facet_row.unit_epoch>epoch_number THEN
            RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
                'error',jsonb_build_object('code','MEMORY_VECTOR_PUBLISH_MEMBERS_INVALID',
                    'message','Vector publication facet differs','status',409));
        END IF;
        source_eligible := facet_row.facet_state<>'SUPPRESSED'
            AND (facet_row.unit_state='ELIGIBLE'
                 OR (facet_row.purpose='QUERY' AND facet_row.unit_state='REGISTERED'))
            AND facet_row.unit_revision=facet_row.source_revision
            AND facet_row.source_state='ACTIVE'
            AND facet_row.occurrence_id IS NOT NULL;
        IF source_eligible THEN
            IF item_result->>'status'<>'PUBLISHED' THEN
                RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
                    'error',jsonb_build_object('code','MEMORY_VECTOR_REF_NOT_PUBLISHED',
                        'message','Eligible vector ref was not published','status',409));
            END IF;
            IF facet_row.facet_state='EMBEDDED' THEN
                UPDATE __forest_module_schema__.embedding_facets
                SET state='READY',index_ref_id=facet_row.facet_id,
                    published_at=clock_timestamp(),updated_at=clock_timestamp()
                WHERE bot_id=requested_bot AND facet_id=facet_row.facet_id;
            ELSIF facet_row.index_ref_id IS DISTINCT FROM facet_row.facet_id THEN
                RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
                    'error',jsonb_build_object('code','MEMORY_VECTOR_REF_CONFLICT',
                        'message','Published vector ref differs','status',409));
            END IF;
            ready_count := ready_count+1;
        ELSE
            INSERT INTO __forest_module_schema__.vector_index_deletions(
                bot_id,index_ref_id,space_id,source_unit_id,deletion_epoch)
            VALUES(requested_bot,facet_row.facet_id,facet_row.space_id,
                facet_row.source_unit_id,epoch_number)
            ON CONFLICT (bot_id,index_ref_id) DO NOTHING;
            IF facet_row.facet_state IN ('EMBEDDED','READY') THEN
                UPDATE __forest_module_schema__.embedding_facets
                SET state='SUPPRESSED',index_ref_id=NULL,vector_digest=NULL,
                    result_payload_id=NULL,result_payload_digest=NULL,
                    published_at=NULL,updated_at=clock_timestamp()
                WHERE bot_id=requested_bot AND facet_id=facet_row.facet_id;
            END IF;
            deletion_count := deletion_count+1;
        END IF;
    END LOOP;
    IF seen_count<>claim_row.item_count THEN
        RAISE EXCEPTION 'Vector publication membership changed under lock'
            USING ERRCODE='55000';
    END IF;
    result_value := jsonb_build_object('protocol','forest.memory-vector-published/1',
        'attempt_id',attempt_row.attempt_id,'ready_count',ready_count,
        'deletion_count',deletion_count);
    INSERT INTO __forest_module_schema__.vector_publish_operations(
        bot_id,operation_id,input_digest,result_payload)
    VALUES(requested_bot,requested_operation,requested_digest,result_value);
    RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',true,
        'value',result_value);
EXCEPTION WHEN invalid_text_representation OR numeric_value_out_of_range THEN
    RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
        'error',jsonb_build_object('code','MEMORY_VECTOR_PUBLISH_INVALID',
            'message','Vector publication evidence is invalid','status',422));
END;
$$;

CREATE FUNCTION memory_settle_vector_publish(context jsonb, input jsonb)
RETURNS jsonb LANGUAGE plpgsql VOLATILE SET search_path = pg_catalog AS $$
DECLARE
    requested_bot text := context->>'bot_id';
    requested_token text := input->>'claim_token';
    prior_row __forest_module_schema__.vector_work_settlements%ROWTYPE;
    work_row __forest_module_schema__.vector_publish_batches%ROWTYPE;
    result_value jsonb;
BEGIN
    IF input->>'protocol' IS DISTINCT FROM 'forest.owned-work-settle-request/1'
       OR requested_bot IS NULL OR requested_token IS NULL
       OR input->>'work_id' IS NULL
       OR input->>'attempt' !~ '^[1-9][0-9]*$'
       OR input#>>'{outcome,status}' NOT IN ('SUCCEEDED','FAILED') THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object('code','MEMORY_VECTOR_SETTLEMENT_INVALID',
                'message','Vector work settlement is invalid','status',422));
    END IF;
    SELECT * INTO prior_row FROM __forest_module_schema__.vector_work_settlements
    WHERE bot_id=requested_bot AND claim_token=requested_token;
    IF FOUND THEN
        IF prior_row.input_payload=input THEN RETURN prior_row.result_payload; END IF;
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object('code','MEMORY_VECTOR_SETTLEMENT_CONFLICT',
                'message','Vector work was settled differently','status',409));
    END IF;
    SELECT * INTO work_row FROM __forest_module_schema__.vector_publish_batches
    WHERE bot_id=requested_bot AND claim_token=requested_token FOR UPDATE;
    IF work_row IS NULL OR work_row.state<>'CLAIMED'
       OR work_row.attempt_id IS DISTINCT FROM input->>'work_id'
       OR work_row.attempt_count IS DISTINCT FROM (input->>'attempt')::integer THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object('code','MEMORY_VECTOR_PUBLISH_CLAIM_LOST',
                'message','Vector publish claim is no longer current','status',409));
    END IF;
    IF input#>>'{outcome,status}'='SUCCEEDED' THEN
        IF input#>>'{outcome,result,attempt_id}' IS DISTINCT FROM work_row.attempt_id
           OR EXISTS (
               SELECT 1 FROM __forest_module_schema__.embedding_claim_items ci
               JOIN __forest_module_schema__.embedding_batch_attempts a
                 ON a.bot_id=ci.bot_id AND a.claim_id=ci.claim_id
               JOIN __forest_module_schema__.embedding_facets f
                 ON f.bot_id=ci.bot_id AND f.facet_id=ci.facet_id
               WHERE a.bot_id=requested_bot AND a.attempt_id=work_row.attempt_id
                 AND f.state NOT IN ('READY','SUPPRESSED')
           ) THEN
            RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
                'error',jsonb_build_object('code','MEMORY_VECTOR_PUBLISH_NOT_COMPLETE',
                    'message','Vector publish result was not committed','status',409));
        END IF;
        UPDATE __forest_module_schema__.vector_publish_batches
        SET state='SETTLED',claim_token=NULL,claim_payload=NULL,lease_deadline=NULL,
            updated_at=clock_timestamp()
        WHERE bot_id=requested_bot AND attempt_id=work_row.attempt_id;
    ELSE
        UPDATE __forest_module_schema__.vector_publish_batches
        SET state='PENDING',claim_token=NULL,claim_payload=NULL,lease_deadline=NULL,
            next_attempt_at=clock_timestamp()+
                (LEAST(power(2,LEAST(attempt_count,8)),300)*interval '1 second'),
            updated_at=clock_timestamp()
        WHERE bot_id=requested_bot AND attempt_id=work_row.attempt_id;
    END IF;
    result_value := jsonb_build_object('protocol','forest.domain-result/1','ok',true,
        'value',jsonb_build_object('protocol','forest.owned-work-settled/1',
            'work_id',work_row.attempt_id));
    INSERT INTO __forest_module_schema__.vector_work_settlements(
        bot_id,claim_token,input_payload,result_payload)
    VALUES(requested_bot,requested_token,input,result_value);
    RETURN result_value;
EXCEPTION WHEN invalid_text_representation OR numeric_value_out_of_range THEN
    RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
        'error',jsonb_build_object('code','MEMORY_VECTOR_SETTLEMENT_INVALID',
            'message','Vector work settlement is invalid','status',422));
END;
$$;

CREATE FUNCTION memory_claim_vector_deletion(context jsonb, input jsonb)
RETURNS jsonb LANGUAGE plpgsql VOLATILE SET search_path = pg_catalog AS $$
DECLARE
    requested_bot text := context->>'bot_id';
    requested_token text := input->>'claim_token';
    requested_lease integer := (input->>'lease_seconds')::integer;
    work_row __forest_module_schema__.vector_index_deletions%ROWTYPE;
    task_payload jsonb;
BEGIN
    IF input->>'protocol' IS DISTINCT FROM 'forest.owned-work-claim-request/1'
       OR requested_bot IS NULL OR requested_token IS NULL
       OR octet_length(requested_token) NOT BETWEEN 1 AND 256
       OR requested_lease IS NULL OR requested_lease NOT BETWEEN 3 AND 86400 THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object('code','MEMORY_VECTOR_DELETE_CLAIM_INVALID',
                'message','Vector deletion claim is invalid','status',422));
    END IF;
    SELECT * INTO work_row FROM __forest_module_schema__.vector_index_deletions
    WHERE bot_id=requested_bot AND claim_token=requested_token;
    IF FOUND THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',true,
            'value',work_row.claim_payload);
    END IF;
    SELECT * INTO work_row FROM __forest_module_schema__.vector_index_deletions
    WHERE bot_id=requested_bot AND state='PENDING'
      AND next_attempt_at<=clock_timestamp()
      AND (claim_token IS NULL OR lease_deadline<=clock_timestamp())
    ORDER BY next_attempt_at,index_ref_id LIMIT 1 FOR UPDATE SKIP LOCKED;
    IF NOT FOUND THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',true,'value',NULL);
    END IF;
    task_payload := jsonb_build_object(
        'protocol','forest.owned-work-claim/1',
        'work_id',work_row.index_ref_id,'claim_token',requested_token,
        'attempt',work_row.attempt_count+1,
        'operation_id','memory-vector-delete:'||encode(sha256(convert_to(
            requested_bot||'/'||requested_token,'UTF8')),'hex'),
        'causal_root_kind','memory.vector.deletion',
        'causal_root_id',work_row.index_ref_id,
        'payload',jsonb_build_object(
            'index_ref_id',work_row.index_ref_id,'space_id',work_row.space_id));
    UPDATE __forest_module_schema__.vector_index_deletions
    SET claim_token=requested_token,claim_payload=task_payload,
        lease_deadline=clock_timestamp()+(requested_lease*interval '1 second'),
        attempt_count=attempt_count+1
    WHERE bot_id=requested_bot AND index_ref_id=work_row.index_ref_id;
    RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',true,
        'value',task_payload);
EXCEPTION WHEN invalid_text_representation OR numeric_value_out_of_range THEN
    RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
        'error',jsonb_build_object('code','MEMORY_VECTOR_DELETE_CLAIM_INVALID',
            'message','Vector deletion claim is invalid','status',422));
END;
$$;

CREATE FUNCTION memory_settle_vector_deletion(context jsonb, input jsonb)
RETURNS jsonb LANGUAGE plpgsql VOLATILE SET search_path = pg_catalog AS $$
DECLARE
    requested_bot text := context->>'bot_id';
    requested_token text := input->>'claim_token';
    prior_row __forest_module_schema__.vector_work_settlements%ROWTYPE;
    work_row __forest_module_schema__.vector_index_deletions%ROWTYPE;
    result_value jsonb;
BEGIN
    IF input->>'protocol' IS DISTINCT FROM 'forest.owned-work-settle-request/1'
       OR requested_bot IS NULL OR requested_token IS NULL
       OR input->>'work_id' IS NULL
       OR input->>'attempt' !~ '^[1-9][0-9]*$'
       OR input#>>'{outcome,status}' NOT IN ('SUCCEEDED','FAILED') THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object('code','MEMORY_VECTOR_SETTLEMENT_INVALID',
                'message','Vector work settlement is invalid','status',422));
    END IF;
    SELECT * INTO prior_row FROM __forest_module_schema__.vector_work_settlements
    WHERE bot_id=requested_bot AND claim_token=requested_token;
    IF FOUND THEN
        IF prior_row.input_payload=input THEN RETURN prior_row.result_payload; END IF;
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object('code','MEMORY_VECTOR_SETTLEMENT_CONFLICT',
                'message','Vector work was settled differently','status',409));
    END IF;
    SELECT * INTO work_row FROM __forest_module_schema__.vector_index_deletions
    WHERE bot_id=requested_bot AND claim_token=requested_token FOR UPDATE;
    IF work_row IS NULL OR work_row.state<>'PENDING'
       OR work_row.index_ref_id IS DISTINCT FROM input->>'work_id'
       OR work_row.attempt_count IS DISTINCT FROM (input->>'attempt')::integer THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object('code','MEMORY_VECTOR_DELETE_CLAIM_LOST',
                'message','Vector deletion claim is no longer current','status',409));
    END IF;
    IF input#>>'{outcome,status}'='SUCCEEDED' THEN
        IF input#>>'{outcome,result,index_ref_id}' IS DISTINCT FROM work_row.index_ref_id
           OR input#>>'{outcome,result,deleted}' IS DISTINCT FROM 'true' THEN
            RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
                'error',jsonb_build_object('code','MEMORY_VECTOR_DELETE_RESULT_INVALID',
                    'message','Vector deletion result differs','status',409));
        END IF;
        UPDATE __forest_module_schema__.vector_index_deletions
        SET state='DELETED',claim_token=NULL,claim_payload=NULL,lease_deadline=NULL,
            deleted_at=clock_timestamp()
        WHERE bot_id=requested_bot AND index_ref_id=work_row.index_ref_id;
    ELSE
        UPDATE __forest_module_schema__.vector_index_deletions
        SET claim_token=NULL,claim_payload=NULL,lease_deadline=NULL,
            next_attempt_at=clock_timestamp()+
                (LEAST(power(2,LEAST(attempt_count,8)),300)*interval '1 second')
        WHERE bot_id=requested_bot AND index_ref_id=work_row.index_ref_id;
    END IF;
    result_value := jsonb_build_object('protocol','forest.domain-result/1','ok',true,
        'value',jsonb_build_object('protocol','forest.owned-work-settled/1',
            'work_id',work_row.index_ref_id));
    INSERT INTO __forest_module_schema__.vector_work_settlements(
        bot_id,claim_token,input_payload,result_payload)
    VALUES(requested_bot,requested_token,input,result_value);
    RETURN result_value;
EXCEPTION WHEN invalid_text_representation OR numeric_value_out_of_range THEN
    RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
        'error',jsonb_build_object('code','MEMORY_VECTOR_SETTLEMENT_INVALID',
            'message','Vector work settlement is invalid','status',422));
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
    IF OLD.eligibility_state='REGISTERED' AND NEW.eligibility_state IN ('ADMITTED','ELIGIBLE','SUPPRESSED')
       THEN RETURN NEW; END IF;
    IF OLD.eligibility_state='ADMITTED' AND NEW.eligibility_state IN ('ELIGIBLE','SUPPRESSED')
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
    IF OLD.state='PENDING' AND NEW.state='PENDING'
       AND NEW.next_attempt_at>=OLD.next_attempt_at
       AND ROW(NEW.attempt_count,NEW.current_attempt_id,NEW.index_ref_id,
               NEW.vector_digest,NEW.result_payload_id,NEW.result_payload_digest,
               NEW.published_at)
           IS NOT DISTINCT FROM
           ROW(OLD.attempt_count,OLD.current_attempt_id,OLD.index_ref_id,
               OLD.vector_digest,OLD.result_payload_id,OLD.result_payload_digest,
               OLD.published_at) THEN RETURN NEW; END IF;
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
    IF ROW(NEW.bot_id,NEW.attempt_id,NEW.claim_id,
           NEW.effect_operation_id,NEW.executor_component_id,
           NEW.executor_generation_id,NEW.fence_graph_id,NEW.fence_graph_revision,
           NEW.accepted_deletion_epoch,NEW.effect_wire_payload_id,
           NEW.effect_wire_payload_digest,NEW.reservation_id,NEW.deadline_at,NEW.prepared_at)
       IS DISTINCT FROM
       ROW(OLD.bot_id,OLD.attempt_id,OLD.claim_id,
           OLD.effect_operation_id,OLD.executor_component_id,
           OLD.executor_generation_id,OLD.fence_graph_id,OLD.fence_graph_revision,
           OLD.accepted_deletion_epoch,OLD.effect_wire_payload_id,
           OLD.effect_wire_payload_digest,OLD.reservation_id,OLD.deadline_at,OLD.prepared_at) THEN
        RAISE EXCEPTION 'Embedding attempt identity is immutable' USING ERRCODE='55000';
    END IF;
    IF OLD.state='PREPARED' AND NEW.state IN ('DISPATCHED','NOT_SENT') THEN RETURN NEW; END IF;
    IF OLD.state='DISPATCHED' AND NEW.state IN (
        'SUCCEEDED','NOT_SENT','UNKNOWN','FAILED'
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
    IF OLD.state='PENDING' AND NEW.state='PENDING'
       AND NEW.deleted_at IS NULL AND NEW.next_attempt_at>=OLD.next_attempt_at THEN
        IF OLD.claim_token IS NULL AND NEW.claim_token IS NOT NULL
           AND NEW.attempt_count=OLD.attempt_count+1 THEN RETURN NEW; END IF;
        IF OLD.claim_token IS NOT NULL AND NEW.claim_token IS NULL
           AND NEW.attempt_count=OLD.attempt_count THEN RETURN NEW; END IF;
        IF OLD.claim_token IS NOT NULL AND NEW.claim_token IS NOT NULL
           AND OLD.claim_token IS DISTINCT FROM NEW.claim_token
           AND OLD.lease_deadline<=clock_timestamp()
           AND NEW.attempt_count=OLD.attempt_count+1 THEN RETURN NEW; END IF;
    END IF;
    IF OLD.state='PENDING' AND NEW.state='DELETED'
       AND NEW.deleted_at IS NOT NULL AND NEW.claim_token IS NULL
       AND NEW.lease_deadline IS NULL THEN
        RETURN NEW;
    END IF;
    IF NEW IS NOT DISTINCT FROM OLD THEN RETURN NEW; END IF;
    RAISE EXCEPTION 'Invalid vector index deletion transition' USING ERRCODE='55000';
END;
$$;

CREATE FUNCTION guard_vector_publish_batch() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF ROW(NEW.bot_id,NEW.attempt_id,NEW.created_at) IS DISTINCT FROM
       ROW(OLD.bot_id,OLD.attempt_id,OLD.created_at) THEN
        RAISE EXCEPTION 'Vector publish work identity is immutable' USING ERRCODE='55000';
    END IF;
    IF NEW.updated_at<OLD.updated_at OR NEW.next_attempt_at<OLD.next_attempt_at THEN
        RAISE EXCEPTION 'Vector publish work moved backward' USING ERRCODE='55000';
    END IF;
    IF OLD.state='PENDING' AND NEW.state='CLAIMED'
       AND NEW.attempt_count=OLD.attempt_count+1 THEN RETURN NEW; END IF;
    IF OLD.state='CLAIMED' AND NEW.state='CLAIMED'
       AND OLD.lease_deadline<=clock_timestamp()
       AND NEW.claim_token IS DISTINCT FROM OLD.claim_token
       AND NEW.attempt_count=OLD.attempt_count+1 THEN RETURN NEW; END IF;
    IF OLD.state='CLAIMED' AND NEW.state IN ('PENDING','SETTLED')
       AND NEW.claim_token IS NULL AND NEW.attempt_count=OLD.attempt_count THEN
        RETURN NEW;
    END IF;
    IF NEW IS NOT DISTINCT FROM OLD THEN RETURN NEW; END IF;
    RAISE EXCEPTION 'Invalid vector publish work transition' USING ERRCODE='55000';
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

CREATE FUNCTION guard_embedding_claim() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF ROW(NEW.bot_id,NEW.claim_id,NEW.purpose,NEW.space_id,
           NEW.source_epoch_high_watermark,NEW.lease_seconds,NEW.lease_deadline,
           NEW.item_limit,NEW.item_count,NEW.claim_payload,NEW.created_at)
       IS DISTINCT FROM
       ROW(OLD.bot_id,OLD.claim_id,OLD.purpose,OLD.space_id,
           OLD.source_epoch_high_watermark,OLD.lease_seconds,OLD.lease_deadline,
           OLD.item_limit,OLD.item_count,OLD.claim_payload,OLD.created_at) THEN
        RAISE EXCEPTION 'Embedding claim identity is immutable' USING ERRCODE='55000';
    END IF;
    IF OLD.state='CLAIMED' AND NEW.state='EXPIRED' AND NEW.settled_at IS NULL THEN
        RETURN NEW;
    END IF;
    IF OLD.state='CLAIMED' AND NEW.state='SETTLED' AND NEW.settled_at IS NOT NULL THEN
        RETURN NEW;
    END IF;
    IF NEW IS NOT DISTINCT FROM OLD THEN RETURN NEW; END IF;
    RAISE EXCEPTION 'Invalid embedding claim transition' USING ERRCODE='55000';
END;
$$;

CREATE TRIGGER memory_domain_immutable BEFORE UPDATE OR DELETE ON memory_domains
    FOR EACH ROW EXECUTE FUNCTION guard_memory_domain();
CREATE TRIGGER memory_source_guard BEFORE UPDATE OR DELETE ON memory_sources
    FOR EACH ROW EXECUTE FUNCTION guard_memory_source();
CREATE TRIGGER source_unit_guard BEFORE UPDATE OR DELETE ON source_units
    FOR EACH ROW EXECUTE FUNCTION guard_source_unit();
CREATE TRIGGER unavailable_source_events_immutable BEFORE UPDATE OR DELETE ON unavailable_source_events
    FOR EACH ROW EXECUTE FUNCTION reject_immutable_row();
CREATE TRIGGER source_occurrence_guard BEFORE UPDATE OR DELETE ON source_occurrences
    FOR EACH ROW EXECUTE FUNCTION guard_occurrence();
CREATE TRIGGER relation_contribution_guard BEFORE UPDATE OR DELETE ON relation_contributions
    FOR EACH ROW EXECUTE FUNCTION guard_relation_contribution();
CREATE TRIGGER memory_query_guard BEFORE UPDATE OR DELETE ON memory_queries
    FOR EACH ROW EXECUTE FUNCTION guard_memory_query();
CREATE TRIGGER embedding_facet_guard BEFORE UPDATE OR DELETE ON embedding_facets
    FOR EACH ROW EXECUTE FUNCTION guard_embedding_facet();
CREATE TRIGGER embedding_attempt_guard BEFORE UPDATE OR DELETE ON embedding_batch_attempts
    FOR EACH ROW EXECUTE FUNCTION guard_embedding_attempt();
CREATE TRIGGER vector_index_deletion_guard BEFORE UPDATE OR DELETE ON vector_index_deletions
    FOR EACH ROW EXECUTE FUNCTION guard_vector_index_deletion();
CREATE TRIGGER vector_publish_batch_guard BEFORE UPDATE OR DELETE ON vector_publish_batches
    FOR EACH ROW EXECUTE FUNCTION guard_vector_publish_batch();
CREATE TRIGGER embedding_claim_guard BEFORE UPDATE OR DELETE ON embedding_claims
    FOR EACH ROW EXECUTE FUNCTION guard_embedding_claim();
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
CREATE TRIGGER source_forget_operations_immutable BEFORE UPDATE OR DELETE ON source_forget_operations
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
CREATE TRIGGER query_facet_preparations_immutable
    BEFORE UPDATE OR DELETE ON query_facet_preparations
    FOR EACH ROW EXECUTE FUNCTION reject_immutable_row();
CREATE TRIGGER retrieval_bundles_immutable BEFORE UPDATE OR DELETE ON retrieval_bundles
    FOR EACH ROW EXECUTE FUNCTION reject_immutable_row();
CREATE TRIGGER retrieval_items_immutable BEFORE UPDATE OR DELETE ON retrieval_items
    FOR EACH ROW EXECUTE FUNCTION reject_immutable_row();
CREATE TRIGGER retrieval_projections_immutable BEFORE UPDATE OR DELETE ON retrieval_projections
    FOR EACH ROW EXECUTE FUNCTION reject_immutable_row();
CREATE TRIGGER retrieval_matches_immutable BEFORE UPDATE OR DELETE ON retrieval_matches
    FOR EACH ROW EXECUTE FUNCTION reject_immutable_row();

CREATE FUNCTION memory_claim_domain_bind(context jsonb, input jsonb)
RETURNS jsonb LANGUAGE plpgsql VOLATILE SET search_path = pg_catalog AS $$
DECLARE
    requested_bot text := context->>'bot_id';
    requested_token text := input->>'claim_token';
    requested_lease integer := (input->>'lease_seconds')::integer;
    work_row __forest_module_schema__.memory_domain_bind_work%ROWTYPE;
BEGIN
    IF input->>'protocol' IS DISTINCT FROM 'forest.owned-work-claim-request/1'
       OR requested_bot IS NULL OR requested_token IS NULL
       OR octet_length(requested_token) NOT BETWEEN 1 AND 256
       OR requested_lease IS NULL OR requested_lease NOT BETWEEN 3 AND 86400 THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object('code','MEMORY_DOMAIN_CLAIM_INVALID',
                'message','Memory domain claim is invalid','status',422));
    END IF;
    INSERT INTO __forest_module_schema__.memory_domain_bind_work(bot_id,state)
    VALUES(requested_bot,CASE WHEN EXISTS (
        SELECT 1 FROM __forest_module_schema__.memory_domains WHERE bot_id=requested_bot
    ) THEN 'READY' ELSE 'PENDING' END)
    ON CONFLICT (bot_id) DO NOTHING;
    UPDATE __forest_module_schema__.memory_domain_bind_work
    SET state='PENDING',claim_token=NULL,lease_deadline=NULL,
        next_attempt_at=clock_timestamp(),updated_at=clock_timestamp()
    WHERE bot_id=requested_bot AND state='CLAIMED' AND lease_deadline<=clock_timestamp();
    UPDATE __forest_module_schema__.memory_domain_bind_work
    SET state='READY',updated_at=clock_timestamp()
    WHERE bot_id=requested_bot AND state='PENDING' AND EXISTS (
        SELECT 1 FROM __forest_module_schema__.memory_domains WHERE bot_id=requested_bot
    );
    SELECT * INTO work_row FROM __forest_module_schema__.memory_domain_bind_work
    WHERE bot_id=requested_bot AND state='PENDING'
      AND next_attempt_at<=clock_timestamp() FOR UPDATE SKIP LOCKED;
    IF work_row IS NULL THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',true,'value',NULL);
    END IF;
    UPDATE __forest_module_schema__.memory_domain_bind_work
    SET state='CLAIMED',claim_token=requested_token,
        lease_deadline=clock_timestamp()+(requested_lease*interval '1 second'),
        attempt_count=attempt_count+1,updated_at=clock_timestamp()
    WHERE bot_id=requested_bot RETURNING * INTO work_row;
    RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',true,
        'value',jsonb_build_object(
            'protocol','forest.owned-work-claim/1',
            'work_id','memory.domain',
            'claim_token',work_row.claim_token,
            'attempt',work_row.attempt_count,
            'operation_id','memory-domain-bind:'||work_row.attempt_count,
            'causal_root_kind','memory.domain',
            'causal_root_id',requested_bot,
            'payload',jsonb_build_object(
                'protocol','forest.memory-domain-ensure/1',
                'request_id','domain:'||encode(sha256(convert_to(requested_bot,'UTF8')),'hex')
            )
        ));
END;
$$;

CREATE FUNCTION memory_settle_domain_bind(context jsonb, input jsonb)
RETURNS jsonb LANGUAGE plpgsql VOLATILE SET search_path = pg_catalog AS $$
DECLARE
    requested_bot text := context->>'bot_id';
    old_input jsonb;
    old_result jsonb;
    result_value jsonb;
    work_row __forest_module_schema__.memory_domain_bind_work%ROWTYPE;
    succeeded boolean := input#>>'{outcome,status}'='SUCCEEDED'
        AND input#>>'{outcome,result,bound}'='true';
BEGIN
    SELECT input_payload,result_payload INTO old_input,old_result
    FROM __forest_module_schema__.memory_domain_bind_settlements
    WHERE claim_token=input->>'claim_token';
    IF FOUND THEN
        IF old_input=input THEN RETURN old_result; END IF;
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object('code','MEMORY_DOMAIN_SETTLEMENT_CONFLICT',
                'message','Memory domain claim was settled differently','status',409));
    END IF;
    SELECT * INTO work_row FROM __forest_module_schema__.memory_domain_bind_work
    WHERE bot_id=requested_bot FOR UPDATE;
    IF work_row IS NULL OR work_row.state<>'CLAIMED'
       OR input->>'work_id' IS DISTINCT FROM 'memory.domain'
       OR work_row.claim_token IS DISTINCT FROM input->>'claim_token'
       OR work_row.attempt_count IS DISTINCT FROM (input->>'attempt')::integer THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object('code','MEMORY_DOMAIN_CLAIM_LOST',
                'message','Memory domain claim is no longer current','status',409));
    END IF;
    IF succeeded AND EXISTS (
        SELECT 1 FROM __forest_module_schema__.memory_domains WHERE bot_id=requested_bot
    ) THEN
        UPDATE __forest_module_schema__.memory_domain_bind_work
        SET state='READY',claim_token=NULL,lease_deadline=NULL,last_error_code=NULL,
            updated_at=clock_timestamp() WHERE bot_id=requested_bot;
    ELSE
        UPDATE __forest_module_schema__.memory_domain_bind_work
        SET state='PENDING',claim_token=NULL,lease_deadline=NULL,
            next_attempt_at=clock_timestamp()+
                (LEAST(power(2,LEAST(attempt_count,8)),300)*interval '1 second'),
            last_error_code=COALESCE(input#>>'{outcome,error,code}',
                'MEMORY_DOMAIN_BIND_RETRY'),updated_at=clock_timestamp()
        WHERE bot_id=requested_bot;
    END IF;
    result_value := jsonb_build_object('protocol','forest.domain-result/1','ok',true,
        'value',jsonb_build_object('protocol','forest.owned-work-settled/1',
            'work_id','memory.domain'));
    INSERT INTO __forest_module_schema__.memory_domain_bind_settlements(
        claim_token,input_payload,result_payload)
    VALUES(input->>'claim_token',input,result_value);
    RETURN result_value;
EXCEPTION WHEN invalid_text_representation OR numeric_value_out_of_range THEN
    RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
        'error',jsonb_build_object('code','MEMORY_DOMAIN_SETTLEMENT_INVALID',
            'message','Memory domain settlement is invalid','status',422));
END;
$$;

CREATE FUNCTION memory_claim_facet_preparation(context jsonb, input jsonb)
RETURNS jsonb LANGUAGE plpgsql VOLATILE SET search_path = pg_catalog AS $$
DECLARE
    requested_bot text := context->>'bot_id';
    requested_token text := input->>'claim_token';
    requested_lease integer := (input->>'lease_seconds')::integer;
    work_row __forest_module_schema__.facet_preparation_work%ROWTYPE;
    unit_row __forest_module_schema__.source_units%ROWTYPE;
    domain_row __forest_module_schema__.memory_domains%ROWTYPE;
BEGIN
    IF input->>'protocol' IS DISTINCT FROM 'forest.owned-work-claim-request/1'
       OR requested_bot IS NULL OR requested_token IS NULL
       OR octet_length(requested_token) NOT BETWEEN 1 AND 256
       OR requested_lease IS NULL OR requested_lease NOT BETWEEN 3 AND 86400 THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object('code','MEMORY_FACET_CLAIM_INVALID',
                'message','Facet preparation claim is invalid','status',422));
    END IF;
    SELECT * INTO domain_row FROM __forest_module_schema__.memory_domains
    WHERE bot_id=requested_bot;
    IF domain_row IS NULL THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',true,'value',NULL);
    END IF;
    UPDATE __forest_module_schema__.facet_preparation_work
    SET state='PENDING',claim_token=NULL,lease_deadline=NULL,
        next_attempt_at=clock_timestamp(),updated_at=clock_timestamp(),
        last_error_code='MEMORY_FACET_LEASE_EXPIRED'
    WHERE bot_id=requested_bot AND state='CLAIMED' AND lease_deadline<=clock_timestamp();
    SELECT w.* INTO work_row FROM __forest_module_schema__.facet_preparation_work w
    WHERE w.bot_id=requested_bot AND w.state='PENDING'
      AND w.next_attempt_at<=clock_timestamp()
    ORDER BY w.next_attempt_at,w.source_unit_id
    FOR UPDATE OF w SKIP LOCKED LIMIT 1;
    IF work_row IS NULL THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',true,'value',NULL);
    END IF;
    SELECT * INTO unit_row FROM __forest_module_schema__.source_units
    WHERE bot_id=requested_bot AND source_unit_id=work_row.source_unit_id;
    UPDATE __forest_module_schema__.facet_preparation_work
    SET state='CLAIMED',claim_token=requested_token,
        lease_deadline=clock_timestamp()+(requested_lease*interval '1 second'),
        attempt_count=attempt_count+1,updated_at=clock_timestamp()
    WHERE bot_id=requested_bot AND source_unit_id=work_row.source_unit_id
    RETURNING * INTO work_row;
    RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',true,
        'value',jsonb_build_object(
            'protocol','forest.owned-work-claim/1',
            'work_id',work_row.source_unit_id,
            'claim_token',work_row.claim_token,
            'attempt',work_row.attempt_count,
            'operation_id','memory-facet-work:'||encode(sha256(convert_to(
                requested_bot||'/'||work_row.source_unit_id||'/'||work_row.attempt_count,
                'UTF8')),'hex'),
            'causal_root_kind','memory.facet',
            'causal_root_id',work_row.source_unit_id,
            'payload',jsonb_build_object(
                'source_unit_id',work_row.source_unit_id,
                'source_revision',unit_row.source_revision,
                'space_id',domain_row.space_id
            )
        ));
END;
$$;

CREATE FUNCTION memory_settle_facet_preparation(context jsonb, input jsonb)
RETURNS jsonb LANGUAGE plpgsql VOLATILE SET search_path = pg_catalog AS $$
DECLARE
    requested_bot text := context->>'bot_id';
    old_input jsonb;
    old_result jsonb;
    result_value jsonb;
    work_row __forest_module_schema__.facet_preparation_work%ROWTYPE;
    unit_row __forest_module_schema__.source_units%ROWTYPE;
    outcome jsonb := input->'outcome';
    disposition text := input#>>'{outcome,result,disposition}';
    reported_count integer;
    actual_count integer;
BEGIN
    SELECT input_payload,result_payload INTO old_input,old_result
    FROM __forest_module_schema__.facet_preparation_settlements
    WHERE bot_id=requested_bot AND claim_token=input->>'claim_token';
    IF FOUND THEN
        IF old_input=input THEN RETURN old_result; END IF;
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object('code','MEMORY_FACET_SETTLEMENT_CONFLICT',
                'message','Facet claim was settled differently','status',409));
    END IF;
    SELECT * INTO work_row FROM __forest_module_schema__.facet_preparation_work
    WHERE bot_id=requested_bot AND source_unit_id=input->>'work_id' FOR UPDATE;
    IF work_row IS NULL OR work_row.state<>'CLAIMED'
       OR work_row.claim_token IS DISTINCT FROM input->>'claim_token'
       OR work_row.attempt_count IS DISTINCT FROM (input->>'attempt')::integer THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object('code','MEMORY_FACET_CLAIM_LOST',
                'message','Facet claim is no longer current','status',409));
    END IF;
    SELECT * INTO unit_row FROM __forest_module_schema__.source_units
    WHERE bot_id=requested_bot AND source_unit_id=work_row.source_unit_id;
    IF unit_row.eligibility_state='SUPPRESSED'
       OR (outcome->>'status'='SUCCEEDED' AND disposition='SUPPRESSED') THEN
        UPDATE __forest_module_schema__.facet_preparation_work
        SET state='SUPPRESSED',claim_token=NULL,lease_deadline=NULL,
            last_error_code=NULL,updated_at=clock_timestamp()
        WHERE bot_id=requested_bot AND source_unit_id=work_row.source_unit_id;
    ELSIF outcome->>'status'='SUCCEEDED' AND disposition='FAILED' THEN
        UPDATE __forest_module_schema__.facet_preparation_work
        SET state='FAILED',claim_token=NULL,lease_deadline=NULL,
            last_error_code=COALESCE(outcome#>>'{result,error_code}',
                'MEMORY_FACET_PREPARATION_FAILED'),updated_at=clock_timestamp()
        WHERE bot_id=requested_bot AND source_unit_id=work_row.source_unit_id;
    ELSIF outcome->>'status'='SUCCEEDED' AND disposition='READY'
       AND unit_row.eligibility_state='ELIGIBLE' THEN
        reported_count := (outcome#>>'{result,facet_count}')::integer;
        SELECT count(*) INTO actual_count FROM __forest_module_schema__.embedding_facets
        WHERE bot_id=requested_bot AND source_unit_id=work_row.source_unit_id
          AND source_revision=unit_row.source_revision AND purpose='DOCUMENT';
        IF actual_count=reported_count THEN
            UPDATE __forest_module_schema__.facet_preparation_work
            SET state='READY',claim_token=NULL,lease_deadline=NULL,
                prepared_facet_count=reported_count,last_error_code=NULL,
                updated_at=clock_timestamp()
            WHERE bot_id=requested_bot AND source_unit_id=work_row.source_unit_id;
        END IF;
    END IF;
    IF (SELECT state FROM __forest_module_schema__.facet_preparation_work
        WHERE bot_id=requested_bot AND source_unit_id=work_row.source_unit_id)='CLAIMED' THEN
        UPDATE __forest_module_schema__.facet_preparation_work
        SET state='PENDING',claim_token=NULL,lease_deadline=NULL,
            next_attempt_at=clock_timestamp()+
                (LEAST(power(2,LEAST(attempt_count,8)),300)*interval '1 second'),
            last_error_code=COALESCE(outcome#>>'{error,code}',
                'MEMORY_FACET_PREPARATION_RETRY'),updated_at=clock_timestamp()
        WHERE bot_id=requested_bot AND source_unit_id=work_row.source_unit_id;
    END IF;
    result_value := jsonb_build_object('protocol','forest.domain-result/1','ok',true,
        'value',jsonb_build_object('protocol','forest.owned-work-settled/1',
            'work_id',work_row.source_unit_id));
    INSERT INTO __forest_module_schema__.facet_preparation_settlements(
        bot_id,claim_token,input_payload,result_payload)
    VALUES(requested_bot,input->>'claim_token',input,result_value);
    RETURN result_value;
EXCEPTION WHEN invalid_text_representation OR numeric_value_out_of_range THEN
    RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
        'error',jsonb_build_object('code','MEMORY_FACET_SETTLEMENT_INVALID',
            'message','Facet settlement is invalid','status',422));
END;
$$;

CREATE FUNCTION memory_bind_domain(context jsonb, envelope jsonb)
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
    domain_row __forest_module_schema__.memory_domains%ROWTYPE;
BEGIN
    IF envelope->>'protocol' IS DISTINCT FROM 'forest.uow-step/1'
       OR envelope#>>'{caller,component_id}' IS DISTINCT FROM 'memory.graph'
       OR body->>'protocol' IS DISTINCT FROM 'forest.memory-domain-bind/1'
       OR requested_operation IS NULL
       OR requested_digest !~ '^sha256:[a-f0-9]{64}$'
       OR body->>'space_id' !~ '^sha256:[a-f0-9]{64}$'
       OR body->>'embedding_model_kind' IS DISTINCT FROM 'TEXT_IMAGE'
       OR body->>'model_revision_digest' !~ '^sha256:[a-f0-9]{64}$'
       OR body->>'dimensions' !~ '^[1-9][0-9]*$'
       OR (body->>'dimensions')::integer NOT BETWEEN 1 AND 4096
       OR body->>'normalization' IS DISTINCT FROM 'L2'
       OR body->>'fusion_policy_revision' IS DISTINCT FROM 'independent-facets/1' THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object('code','MEMORY_DOMAIN_INPUT_INVALID',
                'message','Memory embedding domain input is invalid','status',422));
    END IF;
    INSERT INTO __forest_module_schema__.memory_domains(
        bot_id,initialization_operation_id,input_digest,space_id,embedding_model_kind,
        model_revision_digest,dimensions,normalization,fusion_policy_revision)
    VALUES(requested_bot,requested_operation,requested_digest,body->>'space_id',
        body->>'embedding_model_kind',body->>'model_revision_digest',
        (body->>'dimensions')::integer,body->>'normalization',
        body->>'fusion_policy_revision')
    ON CONFLICT (bot_id) DO NOTHING;
    SELECT * INTO domain_row FROM __forest_module_schema__.memory_domains
    WHERE bot_id=requested_bot FOR SHARE;
    IF domain_row IS NULL
       OR domain_row.space_id IS DISTINCT FROM body->>'space_id'
       OR domain_row.embedding_model_kind IS DISTINCT FROM body->>'embedding_model_kind'
       OR domain_row.model_revision_digest IS DISTINCT FROM body->>'model_revision_digest'
       OR domain_row.dimensions IS DISTINCT FROM (body->>'dimensions')::integer
       OR domain_row.normalization IS DISTINCT FROM body->>'normalization'
       OR domain_row.fusion_policy_revision IS DISTINCT FROM body->>'fusion_policy_revision'
       OR (domain_row.initialization_operation_id=requested_operation
           AND domain_row.input_digest IS DISTINCT FROM requested_digest) THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object('code','MEMORY_DOMAIN_IDENTITY_CONFLICT',
                'message','Bot Memory embedding domain is already bound differently','status',409));
    END IF;
    RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',true,
        'value',jsonb_build_object('protocol','forest.memory-domain/1',
            'space_id',domain_row.space_id,
            'embedding_model_kind',domain_row.embedding_model_kind,
            'model_revision_digest',domain_row.model_revision_digest,
            'dimensions',domain_row.dimensions,
            'normalization',domain_row.normalization,
            'fusion_policy_revision',domain_row.fusion_policy_revision));
EXCEPTION WHEN invalid_text_representation OR numeric_value_out_of_range
    OR not_null_violation OR check_violation THEN
    RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
        'error',jsonb_build_object('code','MEMORY_DOMAIN_INPUT_INVALID',
            'message','Memory embedding domain input is invalid','status',422));
END;
$$;

CREATE FUNCTION memory_prepare_facets(context jsonb, envelope jsonb)
RETURNS jsonb LANGUAGE plpgsql VOLATILE SET search_path = pg_catalog AS $$
DECLARE
    body jsonb := envelope->'input';
    facets jsonb := body->'facets';
    requested_bot text := context->>'bot_id';
    requested_operation text := context->>'operation_id';
    requested_digest text := context->>'participant_input_digest';
    source_id_value text := body->>'source_unit_id';
    purpose_value text := CASE body->>'protocol'
        WHEN 'forest.memory-document-facets/1' THEN 'DOCUMENT'
        WHEN 'forest.memory-query-part-facets/1' THEN 'QUERY' END;
    target_kind_value text := CASE purpose_value WHEN 'QUERY' THEN 'QUERY' ELSE 'SOURCE_UNIT' END;
    target_id_value text := CASE purpose_value WHEN 'QUERY' THEN body->>'query_id' ELSE source_id_value END;
    batch_index_value integer;
    source_row __forest_module_schema__.source_units%ROWTYPE;
    part_row __forest_module_schema__.source_parts%ROWTYPE;
    domain_row __forest_module_schema__.memory_domains%ROWTYPE;
    existing_row __forest_module_schema__.embedding_facets%ROWTYPE;
    operation_row __forest_module_schema__.facet_preparation_operations%ROWTYPE;
    facet jsonb;
    reference jsonb;
    modality_value text;
    start_value integer;
    end_value integer;
    byte_count integer;
    facet_count integer;
BEGIN
    IF envelope->>'protocol' IS DISTINCT FROM 'forest.uow-step/1'
       OR envelope#>>'{caller,component_id}' IS DISTINCT FROM 'memory.graph'
       OR purpose_value IS NULL
       OR requested_operation IS NULL OR requested_digest !~ '^sha256:[a-f0-9]{64}$'
       OR source_id_value IS NULL OR octet_length(source_id_value) NOT BETWEEN 1 AND 256
       OR body->>'source_revision' IS NULL
       OR body->>'source_revision' !~ '^[1-9][0-9]*$'
       OR body->>'batch_index' IS NULL OR body->>'batch_index' !~ '^[0-9]+$'
       OR jsonb_typeof(facets) IS DISTINCT FROM 'array'
       OR jsonb_array_length(facets) NOT BETWEEN 0 AND 64
       OR (purpose_value='DOCUMENT' AND jsonb_array_length(facets)=0) THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object('code','MEMORY_FACET_INPUT_INVALID',
                'message','Facet input is invalid','status',422));
    END IF;
    batch_index_value := (body->>'batch_index')::integer;
    facet_count := jsonb_array_length(facets);
    IF batch_index_value NOT BETWEEN 0 AND 65535
       OR (SELECT count(DISTINCT item->>'facet_id') FROM jsonb_array_elements(facets) item)
          <> facet_count THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object('code','MEMORY_FACET_INPUT_INVALID',
                'message','Document facet batch has duplicate or invalid identities','status',422));
    END IF;
    PERFORM pg_advisory_xact_lock(hashtextextended(
        'forest.memory.graph/facet-batch/'||requested_bot||'/'||target_kind_value||'/'||target_id_value||'/'||batch_index_value,0));
    SELECT * INTO operation_row FROM __forest_module_schema__.facet_preparation_operations
    WHERE bot_id=requested_bot AND target_kind=target_kind_value
      AND target_id=target_id_value AND batch_index=batch_index_value;
    IF operation_row IS NOT NULL THEN
        IF operation_row.operation_id IS DISTINCT FROM requested_operation
           OR operation_row.input_digest IS DISTINCT FROM requested_digest
           OR operation_row.facet_count IS DISTINCT FROM facet_count THEN
            RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
                'error',jsonb_build_object('code','MEMORY_FACET_BATCH_CONFLICT',
                    'message','Facet batch was prepared differently','status',409));
        END IF;
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',true,
            'value',jsonb_build_object('protocol','forest.memory-facets-prepared/1',
                'source_unit_id',source_id_value,'batch_index',batch_index_value,
                'facet_count',facet_count));
    END IF;
    SELECT * INTO domain_row FROM __forest_module_schema__.memory_domains
    WHERE bot_id=requested_bot FOR SHARE;
    SELECT u.* INTO source_row FROM __forest_module_schema__.source_units u
    JOIN __forest_module_schema__.memory_sources s
      ON s.bot_id=u.bot_id AND s.source_id=u.source_id AND s.status='ACTIVE'
    WHERE u.bot_id=requested_bot AND u.source_unit_id=source_id_value FOR SHARE OF u;
    IF domain_row IS NULL OR domain_row.space_id IS DISTINCT FROM body->>'space_id'
       OR source_row IS NULL
       OR (purpose_value='DOCUMENT' AND source_row.eligibility_state<>'ELIGIBLE')
       OR (purpose_value='QUERY' AND source_row.eligibility_state NOT IN ('REGISTERED','ELIGIBLE'))
       OR source_row.source_revision IS DISTINCT FROM (body->>'source_revision')::bigint THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object('code','MEMORY_FACET_SOURCE_INELIGIBLE',
                'message','Facet source or embedding space is unavailable','status',409));
    END IF;
    IF purpose_value='QUERY' AND NOT EXISTS (
        SELECT 1 FROM __forest_module_schema__.memory_query_parts qp
        JOIN __forest_module_schema__.memory_queries q USING(bot_id,query_id)
        JOIN __forest_module_schema__.source_occurrences o
          ON o.bot_id=qp.bot_id AND o.occurrence_id=qp.occurrence_id
         AND o.source_unit_id=qp.source_unit_id
        WHERE qp.bot_id=requested_bot AND qp.query_id=target_id_value
          AND qp.query_part_index=batch_index_value AND qp.source_unit_id=source_id_value
          AND qp.source_revision=source_row.source_revision AND qp.part_id=body->>'part_id'
          AND q.state='OPEN' AND o.input_batch_ref=q.input_batch_ref
          AND o.context_revision=q.context_revision AND o.eligibility_state IN ('ADMITTED','ELIGIBLE')
    ) THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object('code','MEMORY_QUERY_PART_MISMATCH',
                'message','Query part differs from its immutable membership','status',409));
    END IF;
    FOR facet IN SELECT value FROM jsonb_array_elements(facets) LOOP
        reference := facet->'input_payload';
        modality_value := facet->>'modality';
        byte_count := (facet->>'input_byte_length')::integer;
        start_value := (facet->>'selector_start_char')::integer;
        end_value := (facet->>'selector_end_char')::integer;
        SELECT * INTO part_row FROM __forest_module_schema__.source_parts
        WHERE bot_id=requested_bot AND source_unit_id=source_id_value
          AND part_id=facet->>'part_id' FOR SHARE;
        IF part_row IS NULL OR octet_length(facet->>'facet_id') NOT BETWEEN 1 AND 256
           OR (purpose_value='QUERY' AND part_row.part_id IS DISTINCT FROM body->>'part_id')
           OR modality_value IS DISTINCT FROM part_row.part_kind
           OR reference->>'payload_id' IS DISTINCT FROM part_row.payload_id
           OR reference->>'digest' IS DISTINCT FROM part_row.payload_digest
           OR reference->>'owner_component_id' IS DISTINCT FROM part_row.payload_owner_component_id
           OR reference->>'owner_kind' IS DISTINCT FROM part_row.payload_owner_kind
           OR reference->>'owner_id' IS DISTINCT FROM part_row.payload_owner_id
           OR facet->>'facet_content_digest' !~ '^sha256:[a-f0-9]{64}$'
           OR byte_count NOT BETWEEN 1 AND 33554432
           OR (modality_value='TEXT' AND
               (start_value IS NULL OR end_value IS NULL OR start_value<0 OR end_value<=start_value))
           OR (modality_value='IMAGE' AND
               (start_value IS NOT NULL OR end_value IS NOT NULL
                OR facet->>'facet_content_digest' IS DISTINCT FROM part_row.payload_digest
                OR byte_count IS DISTINCT FROM part_row.byte_length)) THEN
            RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
                'error',jsonb_build_object('code','MEMORY_FACET_PART_MISMATCH',
                    'message','Facet does not match the source part','status',409));
        END IF;
        INSERT INTO __forest_module_schema__.embedding_facets(
            bot_id,facet_id,purpose,query_id,source_unit_id,source_revision,part_id,modality,
            selector_start_char,selector_end_char,input_payload_id,input_payload_digest,
            facet_content_digest,input_media_type,input_byte_length,space_id,
            accepted_deletion_epoch)
        VALUES(requested_bot,facet->>'facet_id',purpose_value,body->>'query_id',source_id_value,
            source_row.source_revision,part_row.part_id,modality_value,
            start_value,end_value,part_row.payload_id,part_row.payload_digest,
            facet->>'facet_content_digest',part_row.media_type,byte_count,
            domain_row.space_id,source_row.accepted_deletion_epoch)
        ON CONFLICT (bot_id,facet_id) DO NOTHING;
        SELECT * INTO existing_row FROM __forest_module_schema__.embedding_facets
        WHERE bot_id=requested_bot AND facet_id=facet->>'facet_id';
        IF existing_row.purpose IS DISTINCT FROM purpose_value
           OR existing_row.query_id IS DISTINCT FROM body->>'query_id'
           OR existing_row.source_unit_id IS DISTINCT FROM source_id_value
           OR existing_row.source_revision IS DISTINCT FROM source_row.source_revision
           OR existing_row.part_id IS DISTINCT FROM part_row.part_id
           OR existing_row.modality IS DISTINCT FROM modality_value
           OR existing_row.selector_start_char IS DISTINCT FROM start_value
           OR existing_row.selector_end_char IS DISTINCT FROM end_value
           OR existing_row.input_payload_id IS DISTINCT FROM part_row.payload_id
           OR existing_row.input_payload_digest IS DISTINCT FROM part_row.payload_digest
           OR existing_row.input_media_type IS DISTINCT FROM part_row.media_type
           OR existing_row.facet_content_digest IS DISTINCT FROM facet->>'facet_content_digest'
           OR existing_row.input_byte_length IS DISTINCT FROM byte_count
           OR existing_row.space_id IS DISTINCT FROM domain_row.space_id
           OR existing_row.accepted_deletion_epoch IS DISTINCT FROM
              source_row.accepted_deletion_epoch THEN
            RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
                'error',jsonb_build_object('code','MEMORY_FACET_IDENTITY_CONFLICT',
                    'message','Facet identity names different source content','status',409));
        END IF;
    END LOOP;
    INSERT INTO __forest_module_schema__.facet_preparation_operations(
        bot_id,operation_id,input_digest,target_kind,target_id,batch_index,facet_count)
    VALUES(requested_bot,requested_operation,requested_digest,target_kind_value,
        target_id_value,batch_index_value,facet_count);
    RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',true,
        'value',jsonb_build_object('protocol','forest.memory-facets-prepared/1',
            'source_unit_id',source_id_value,'batch_index',batch_index_value,
            'facet_count',facet_count));
EXCEPTION WHEN invalid_text_representation OR numeric_value_out_of_range
    OR not_null_violation OR check_violation THEN
    RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
        'error',jsonb_build_object('code','MEMORY_FACET_INPUT_INVALID',
            'message','Facet input is invalid','status',422));
END;
$$;

CREATE FUNCTION memory_complete_query_facets(context jsonb, envelope jsonb)
RETURNS jsonb LANGUAGE plpgsql VOLATILE SET search_path = pg_catalog AS $$
DECLARE
    body jsonb := envelope->'input';
    requested_bot text := context->>'bot_id';
    requested_query text := body->>'query_id';
    epoch_value jsonb := envelope->'prior'->(body->>'deletion_epoch_step_id');
    part_count integer;
    receipt_count integer;
    facet_count_value integer;
    page_check jsonb;
BEGIN
    IF envelope->>'protocol' IS DISTINCT FROM 'forest.uow-step/1'
       OR envelope#>>'{caller,component_id}' IS DISTINCT FROM 'memory.graph'
       OR body->>'protocol' IS DISTINCT FROM 'forest.memory-query-complete/1'
       OR epoch_value->>'protocol' IS DISTINCT FROM 'forest.payload-epoch/1'
       OR epoch_value->>'deletion_epoch' IS NULL THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object('code','MEMORY_QUERY_INPUT_INVALID',
                'message','Query completion requires a current Payload epoch','status',422));
    END IF;
    PERFORM query_id FROM __forest_module_schema__.memory_queries
    WHERE bot_id=requested_bot AND query_id=requested_query FOR UPDATE;
    PERFORM u.source_unit_id FROM __forest_module_schema__.memory_query_parts qp
    JOIN __forest_module_schema__.source_units u USING(bot_id,source_unit_id)
    JOIN __forest_module_schema__.memory_sources s USING(bot_id,source_id)
    JOIN __forest_module_schema__.source_occurrences o
      ON o.bot_id=qp.bot_id AND o.occurrence_id=qp.occurrence_id
    WHERE qp.bot_id=requested_bot AND qp.query_id=requested_query
    ORDER BY u.source_unit_id,o.occurrence_id FOR SHARE OF u,s,o;
    page_check := __forest_module_schema__.memory_read_query_parts(context,
        jsonb_build_object('query_id',requested_query,'after_part_index',-1,'limit',1));
    IF page_check->>'ok' IS DISTINCT FROM 'true' THEN RETURN page_check; END IF;
    SELECT count(qp.query_part_index),count(op.operation_id),COALESCE(sum(op.facet_count),0)
    INTO part_count,receipt_count,facet_count_value
    FROM __forest_module_schema__.memory_query_parts qp
    LEFT JOIN __forest_module_schema__.facet_preparation_operations op
      ON op.bot_id=qp.bot_id AND op.target_kind='QUERY' AND op.target_id=qp.query_id
     AND op.batch_index=qp.query_part_index
    WHERE qp.bot_id=requested_bot AND qp.query_id=requested_query;
    IF part_count=0 OR part_count<>receipt_count OR facet_count_value>256
       OR facet_count_value IS DISTINCT FROM (body->>'facet_count')::integer
       OR facet_count_value<>(SELECT count(*) FROM __forest_module_schema__.embedding_facets
            WHERE bot_id=requested_bot AND query_id=requested_query AND purpose='QUERY') THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object('code','MEMORY_QUERY_PREPARATION_INCOMPLETE',
                'message','Query part preparation receipts are incomplete','status',409));
    END IF;
    INSERT INTO __forest_module_schema__.query_facet_preparations(
        bot_id,query_id,facet_count,accepted_deletion_epoch)
    VALUES(requested_bot,requested_query,facet_count_value,(epoch_value->>'deletion_epoch')::bigint)
    ON CONFLICT (bot_id,query_id) DO NOTHING;
    RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',true,
        'value',jsonb_build_object('protocol','forest.memory-query-facets/1',
            'query_id',requested_query,'facet_count',facet_count_value,'part_count',part_count,
            'status','PREPARED'));
END;
$$;

CREATE FUNCTION memory_count_eligible_embedding_facets(
    requested_bot text, requested_claim text, expected_space text,
    epoch_number bigint, expected_state text, expected_attempt text
) RETURNS integer LANGUAGE plpgsql VOLATILE SET search_path = pg_catalog AS $$
DECLARE qualified_count integer;
BEGIN
    WITH locked AS MATERIALIZED (
        SELECT f.facet_id
        FROM __forest_module_schema__.embedding_claim_items ci
        JOIN __forest_module_schema__.embedding_facets f
          ON f.bot_id=ci.bot_id AND f.facet_id=ci.facet_id
        JOIN __forest_module_schema__.source_units u
          ON u.bot_id=f.bot_id AND u.source_unit_id=f.source_unit_id
         AND u.source_revision=f.source_revision
         AND (u.eligibility_state='ELIGIBLE'
              OR (f.purpose='QUERY' AND u.eligibility_state='REGISTERED'))
        JOIN __forest_module_schema__.memory_sources s
          ON s.bot_id=u.bot_id AND s.source_id=u.source_id AND s.status='ACTIVE'
        JOIN __forest_module_schema__.source_parts p
          ON p.bot_id=f.bot_id AND p.source_unit_id=f.source_unit_id
         AND p.part_id=f.part_id AND p.payload_id=f.input_payload_id
         AND p.payload_digest=f.input_payload_digest
        JOIN LATERAL (
            SELECT o.occurrence_id
            FROM __forest_module_schema__.source_occurrences o
            WHERE o.bot_id=f.bot_id AND o.source_unit_id=f.source_unit_id
              AND ((f.purpose='DOCUMENT' AND o.eligibility_state='ELIGIBLE')
                   OR (f.purpose='QUERY' AND
                       __forest_module_schema__.memory_query_occurrence_matches(
                           f.bot_id,f.query_id,f.source_unit_id,f.source_revision,
                           f.part_id,o.occurrence_id)))
            ORDER BY o.occurrence_id LIMIT 1 FOR SHARE
        ) occurrence ON true
        WHERE ci.bot_id=requested_bot AND ci.claim_id=requested_claim
          AND f.state=expected_state AND f.space_id=expected_space
          AND f.current_attempt_id IS NOT DISTINCT FROM expected_attempt
          AND f.accepted_deletion_epoch<=epoch_number
        ORDER BY ci.item_index
        FOR UPDATE OF f FOR SHARE OF u,s
    )
    SELECT count(*) INTO qualified_count FROM locked;
    RETURN qualified_count;
END;
$$;

CREATE FUNCTION memory_prepare_embedding_attempt(context jsonb, envelope jsonb)
RETURNS jsonb LANGUAGE plpgsql VOLATILE SET search_path = pg_catalog AS $$
DECLARE
    body jsonb := envelope->'input';
    prior jsonb := envelope->'prior';
    epoch_value jsonb := prior->'payload_epoch';
    budget_value jsonb := prior->'budget';
    effect_value jsonb := prior->'effect';
    permit_value jsonb := effect_value->'permit';
    wire_value jsonb := effect_value->'wire_payload';
    requested_bot text := context->>'bot_id';
    requested_claim text := body->>'claim_id';
    requested_attempt text := body->>'attempt_id';
    requested_effect text := body->>'effect_operation_id';
    epoch_number bigint;
    claim_row __forest_module_schema__.embedding_claims%ROWTYPE;
    existing __forest_module_schema__.embedding_batch_attempts%ROWTYPE;
    domain_space text;
    qualified_count integer;
    updated_count integer;
    output_value jsonb;
BEGIN
    IF envelope->>'protocol' IS DISTINCT FROM 'forest.uow-step/1'
       OR envelope#>>'{caller,component_id}' IS DISTINCT FROM 'memory.graph'
       OR body->>'protocol' IS DISTINCT FROM 'forest.memory-embedding-attempt-prepare/1'
       OR requested_bot IS NULL OR requested_claim IS NULL
       OR requested_attempt IS NULL OR requested_effect IS NULL
       OR epoch_value->>'protocol' IS DISTINCT FROM 'forest.payload-epoch/1'
       OR epoch_value->>'deletion_epoch' !~ '^[0-9]+$'
       OR budget_value->>'protocol' IS DISTINCT FROM 'forest.memory-embedding-budget/1'
       OR budget_value->>'reservation_id' IS DISTINCT FROM body->>'reservation_id'
       OR budget_value->>'state' IS NULL
       OR budget_value->>'state' NOT IN ('RESERVED','CHARGED','UNKNOWN')
       OR effect_value->>'protocol' IS DISTINCT FROM 'forest.effect-command-permit/1'
       OR NULLIF(effect_value->>'authorization_ticket_id','') IS NULL
       OR permit_value->>'protocol' IS DISTINCT FROM 'forest.effect-permit/1'
       OR permit_value->>'effect_operation_id' IS DISTINCT FROM requested_effect
       OR permit_value->>'attempt_id' IS DISTINCT FROM requested_attempt
       OR permit_value->>'attempt_no' IS DISTINCT FROM '1'
       OR permit_value->>'executor_component_id' IS DISTINCT FROM 'embedding.multimodal'
       OR permit_value->>'fence_kind' IS DISTINCT FROM 'runtime.graph'
       OR permit_value->>'wire_payload_id' IS DISTINCT FROM wire_value->>'payload_id'
       OR permit_value->>'wire_payload_digest' IS DISTINCT FROM wire_value->>'digest'
       OR wire_value->>'owner_component_id' IS DISTINCT FROM 'kernel.effect'
       OR wire_value->>'owner_kind' IS DISTINCT FROM 'effect.wire'
       OR wire_value->>'owner_id' IS DISTINCT FROM requested_attempt THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object('code','MEMORY_EMBEDDING_ATTEMPT_INVALID',
                'message','Embedding attempt preparation is invalid','status',422));
    END IF;
    epoch_number := (epoch_value->>'deletion_epoch')::bigint;
    output_value := jsonb_build_object(
        'protocol','forest.memory-embedding-effect-prepared/1',
        'claim_id',requested_claim,'attempt_id',requested_attempt,
        'effect_operation_id',requested_effect,'state','PREPARED',
        'facet_count',NULL,'authorization_ticket_id',
            effect_value->>'authorization_ticket_id',
        'wire_payload',wire_value,'permit',permit_value);
    SELECT * INTO existing FROM __forest_module_schema__.embedding_batch_attempts
    WHERE bot_id=requested_bot AND
      (attempt_id=requested_attempt OR claim_id=requested_claim
       OR effect_operation_id=requested_effect) FOR UPDATE;
    IF FOUND THEN
        IF existing.attempt_id IS DISTINCT FROM requested_attempt
           OR existing.claim_id IS DISTINCT FROM requested_claim
           OR existing.effect_operation_id IS DISTINCT FROM requested_effect
           OR existing.reservation_id IS DISTINCT FROM body->>'reservation_id'
           OR existing.executor_generation_id IS DISTINCT FROM
              permit_value->>'executor_generation_id'
           OR existing.fence_graph_id IS DISTINCT FROM permit_value->>'fence_id'
           OR existing.fence_graph_revision IS DISTINCT FROM
              (permit_value->>'fence_epoch')::bigint
           OR existing.accepted_deletion_epoch IS DISTINCT FROM epoch_number
           OR existing.effect_wire_payload_id IS DISTINCT FROM wire_value->>'payload_id'
           OR existing.effect_wire_payload_digest IS DISTINCT FROM wire_value->>'digest' THEN
            RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
                'error',jsonb_build_object('code','MEMORY_EMBEDDING_ATTEMPT_CONFLICT',
                    'message','Embedding attempt identity differs','status',409));
        END IF;
        SELECT item_count INTO qualified_count FROM __forest_module_schema__.embedding_claims
        WHERE bot_id=requested_bot AND claim_id=requested_claim;
        output_value := jsonb_set(output_value,'{state}',to_jsonb(existing.state));
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',true,
            'value',jsonb_set(output_value,'{facet_count}',to_jsonb(qualified_count)));
    END IF;
    IF budget_value->>'state' IS DISTINCT FROM 'RESERVED' THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object('code','MEMORY_EMBEDDING_BUDGET_NOT_RESERVED',
                'message','Embedding budget is not reserved','status',409));
    END IF;
    SELECT * INTO claim_row FROM __forest_module_schema__.embedding_claims
    WHERE bot_id=requested_bot AND claim_id=requested_claim FOR UPDATE;
    SELECT space_id INTO domain_space FROM __forest_module_schema__.memory_domains
    WHERE bot_id=requested_bot;
    IF claim_row IS NULL OR claim_row.state<>'CLAIMED'
       OR claim_row.lease_deadline<=clock_timestamp()
       OR claim_row.space_id IS DISTINCT FROM domain_space
       OR claim_row.space_id IS DISTINCT FROM body->>'space_id'
       OR claim_row.source_epoch_high_watermark>epoch_number
       OR claim_row.item_count<1 THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object('code','MEMORY_EMBEDDING_CLAIM_LOST',
                'message','Embedding batch claim is no longer eligible','status',409));
    END IF;
    qualified_count := __forest_module_schema__.memory_count_eligible_embedding_facets(
        requested_bot,requested_claim,claim_row.space_id,epoch_number,'PENDING',NULL);
    IF qualified_count<>claim_row.item_count THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object('code','MEMORY_EMBEDDING_SOURCE_CHANGED',
                'message','Embedding batch source changed before handoff','status',409));
    END IF;
    INSERT INTO __forest_module_schema__.embedding_batch_attempts(
        bot_id,attempt_id,claim_id,effect_operation_id,executor_component_id,
        executor_generation_id,fence_graph_id,fence_graph_revision,
        accepted_deletion_epoch,effect_wire_payload_id,effect_wire_payload_digest,
        reservation_id,deadline_at)
    VALUES(requested_bot,requested_attempt,requested_claim,requested_effect,
        'embedding.multimodal',permit_value->>'executor_generation_id',
        permit_value->>'fence_id',(permit_value->>'fence_epoch')::bigint,
        epoch_number,wire_value->>'payload_id',wire_value->>'digest',
        body->>'reservation_id',(permit_value->>'deadline_at')::timestamptz);
    UPDATE __forest_module_schema__.embedding_facets f
    SET state='PREPARED',current_attempt_id=requested_attempt,
        attempt_count=attempt_count+1,updated_at=clock_timestamp()
    FROM __forest_module_schema__.embedding_claim_items ci
    WHERE ci.bot_id=requested_bot AND ci.claim_id=requested_claim
      AND f.bot_id=ci.bot_id AND f.facet_id=ci.facet_id;
    GET DIAGNOSTICS updated_count = ROW_COUNT;
    IF updated_count<>claim_row.item_count THEN
        RAISE EXCEPTION 'Embedding claim membership changed under lock' USING ERRCODE='55000';
    END IF;
    RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',true,
        'value',jsonb_set(output_value,'{facet_count}',to_jsonb(qualified_count)));
EXCEPTION WHEN invalid_text_representation OR numeric_value_out_of_range THEN
    RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
        'error',jsonb_build_object('code','MEMORY_EMBEDDING_ATTEMPT_INVALID',
            'message','Embedding attempt preparation is invalid','status',422));
END;
$$;

CREATE FUNCTION memory_mark_embedding_handoff(context jsonb, envelope jsonb)
RETURNS jsonb LANGUAGE plpgsql VOLATILE SET search_path = pg_catalog AS $$
DECLARE
    body jsonb := envelope->'input';
    epoch_value jsonb := envelope#>'{prior,payload_epoch}';
    effect_value jsonb := envelope#>'{prior,effect}';
    requested_bot text := context->>'bot_id';
    requested_claim text := body->>'claim_id';
    requested_attempt text := body->>'attempt_id';
    requested_effect text := body->>'effect_operation_id';
    requested_handoff text := body->>'handoff_operation_id';
    epoch_number bigint;
    claim_row __forest_module_schema__.embedding_claims%ROWTYPE;
    attempt_row __forest_module_schema__.embedding_batch_attempts%ROWTYPE;
    domain_space text;
    qualified_count integer;
    updated_count integer;
BEGIN
    IF envelope->>'protocol' IS DISTINCT FROM 'forest.uow-step/1'
       OR envelope#>>'{caller,component_id}' IS DISTINCT FROM 'memory.graph'
       OR body->>'protocol' IS DISTINCT FROM 'forest.memory-embedding-handoff/1'
       OR requested_bot IS NULL OR requested_claim IS NULL
       OR requested_attempt IS NULL OR requested_effect IS NULL
       OR requested_handoff IS NULL
       OR epoch_value->>'protocol' IS DISTINCT FROM 'forest.payload-epoch/1'
       OR epoch_value->>'deletion_epoch' !~ '^[0-9]+$'
       OR effect_value->>'effect_operation_id' IS DISTINCT FROM requested_effect
       OR effect_value->>'attempt_id' IS DISTINCT FROM requested_attempt
       OR effect_value->>'handoff_operation_id' IS DISTINCT FROM requested_handoff
       OR effect_value->>'state' IS DISTINCT FROM 'DISPATCHING' THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object('code','MEMORY_EMBEDDING_HANDOFF_INVALID',
                'message','Embedding handoff envelope is invalid','status',422));
    END IF;
    epoch_number := (epoch_value->>'deletion_epoch')::bigint;
    SELECT * INTO claim_row FROM __forest_module_schema__.embedding_claims
    WHERE bot_id=requested_bot AND claim_id=requested_claim FOR UPDATE;
    SELECT * INTO attempt_row FROM __forest_module_schema__.embedding_batch_attempts
    WHERE bot_id=requested_bot AND attempt_id=requested_attempt FOR UPDATE;
    SELECT space_id INTO domain_space FROM __forest_module_schema__.memory_domains
    WHERE bot_id=requested_bot;
    IF claim_row IS NULL OR attempt_row IS NULL
       OR claim_row.state<>'CLAIMED'
       OR claim_row.lease_deadline<=clock_timestamp()
       OR claim_row.space_id IS DISTINCT FROM domain_space
       OR claim_row.source_epoch_high_watermark>epoch_number
       OR claim_row.item_count<1
       OR attempt_row.claim_id IS DISTINCT FROM requested_claim
       OR attempt_row.effect_operation_id IS DISTINCT FROM requested_effect
       OR attempt_row.state<>'PREPARED'
       OR attempt_row.accepted_deletion_epoch IS DISTINCT FROM epoch_number
       OR attempt_row.deadline_at<=clock_timestamp() THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object('code','MEMORY_EMBEDDING_HANDOFF_STALE',
                'message','Embedding claim or attempt is no longer eligible','status',409));
    END IF;
    IF effect_value->>'wire_payload_id' IS DISTINCT FROM attempt_row.effect_wire_payload_id
       OR effect_value->>'wire_payload_digest' IS DISTINCT FROM attempt_row.effect_wire_payload_digest
       OR effect_value->>'executor_component_id' IS DISTINCT FROM attempt_row.executor_component_id
       OR effect_value->>'executor_generation_id' IS DISTINCT FROM attempt_row.executor_generation_id
       OR effect_value->>'fence_id' IS DISTINCT FROM attempt_row.fence_graph_id
       OR effect_value->>'fence_epoch' IS DISTINCT FROM attempt_row.fence_graph_revision::text THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object('code','MEMORY_EMBEDDING_HANDOFF_CONFLICT',
                'message','Effect handoff differs from the frozen embedding attempt','status',409));
    END IF;
    qualified_count := __forest_module_schema__.memory_count_eligible_embedding_facets(
        requested_bot,requested_claim,claim_row.space_id,epoch_number,
        'PREPARED',requested_attempt);
    IF qualified_count<>claim_row.item_count THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object('code','MEMORY_EMBEDDING_SOURCE_CHANGED',
                'message','Embedding batch source changed before handoff','status',409));
    END IF;
    UPDATE __forest_module_schema__.embedding_batch_attempts
    SET state='DISPATCHED',handoff_operation_id=requested_handoff,
        dispatched_at=clock_timestamp()
    WHERE bot_id=requested_bot AND attempt_id=requested_attempt;
    UPDATE __forest_module_schema__.embedding_facets f
    SET state='DISPATCHED',updated_at=clock_timestamp()
    FROM __forest_module_schema__.embedding_claim_items ci
    WHERE ci.bot_id=requested_bot AND ci.claim_id=requested_claim
      AND f.bot_id=ci.bot_id AND f.facet_id=ci.facet_id
      AND f.state='PREPARED' AND f.current_attempt_id=requested_attempt;
    GET DIAGNOSTICS updated_count = ROW_COUNT;
    IF updated_count<>claim_row.item_count THEN
        RAISE EXCEPTION 'Embedding claim membership changed under lock' USING ERRCODE='55000';
    END IF;
    RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',true,
        'value',jsonb_build_object('protocol','forest.memory-embedding-handoff/1',
            'claim_id',requested_claim,'attempt_id',requested_attempt,
            'effect_operation_id',requested_effect,'state','DISPATCHED',
            'facet_count',qualified_count));
EXCEPTION WHEN invalid_text_representation OR numeric_value_out_of_range THEN
    RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
        'error',jsonb_build_object('code','MEMORY_EMBEDDING_HANDOFF_INVALID',
            'message','Embedding handoff envelope is invalid','status',422));
END;
$$;

CREATE FUNCTION memory_abandon_embedding_attempt(context jsonb, envelope jsonb)
RETURNS jsonb LANGUAGE plpgsql VOLATILE SET search_path = pg_catalog AS $$
DECLARE
    body jsonb := envelope->'input';
    effect_value jsonb := envelope#>'{prior,effect}';
    budget_value jsonb := envelope#>'{prior,budget}';
    requested_bot text := context->>'bot_id';
    requested_operation text := context->>'operation_id';
    requested_digest text := context->>'participant_input_digest';
    requested_claim text := body->>'claim_id';
    requested_attempt text := body->>'attempt_id';
    requested_effect text := body->>'effect_operation_id';
    claim_row __forest_module_schema__.embedding_claims%ROWTYPE;
    attempt_row __forest_module_schema__.embedding_batch_attempts%ROWTYPE;
    updated_count integer;
BEGIN
    IF envelope->>'protocol' IS DISTINCT FROM 'forest.uow-step/1'
       OR envelope#>>'{caller,component_id}' IS DISTINCT FROM 'memory.graph'
       OR body->>'protocol' IS DISTINCT FROM 'forest.memory-embedding-abandon/1'
       OR requested_bot IS NULL OR requested_operation IS NULL
       OR requested_digest !~ '^sha256:[a-f0-9]{64}$'
       OR requested_claim IS NULL OR requested_attempt IS NULL
       OR requested_effect IS NULL
       OR effect_value->>'attempt_id' IS DISTINCT FROM requested_attempt
       OR effect_value->>'abandonment_operation_id' IS DISTINCT FROM
          body->>'abandonment_operation_id'
       OR effect_value->>'reason_code' IS DISTINCT FROM body->>'reason_code'
       OR effect_value->>'cancel_effect' IS DISTINCT FROM 'false'
       OR budget_value->>'protocol' IS DISTINCT FROM 'forest.memory-embedding-budget/1'
       OR budget_value->>'reservation_id' IS DISTINCT FROM body->>'reservation_id'
       OR budget_value->>'state' IS DISTINCT FROM 'RELEASED' THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object('code','MEMORY_EMBEDDING_ABANDON_INVALID',
                'message','Embedding abandonment envelope is invalid','status',422));
    END IF;
    SELECT * INTO attempt_row FROM __forest_module_schema__.embedding_batch_attempts
    WHERE bot_id=requested_bot AND attempt_id=requested_attempt FOR UPDATE;
    IF attempt_row IS NULL OR attempt_row.claim_id IS DISTINCT FROM requested_claim
       OR attempt_row.effect_operation_id IS DISTINCT FROM requested_effect
       OR attempt_row.reservation_id IS DISTINCT FROM body->>'reservation_id' THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object('code','MEMORY_EMBEDDING_ATTEMPT_CONFLICT',
                'message','Embedding attempt identity differs','status',409));
    END IF;
    IF attempt_row.state='NOT_SENT'
       AND attempt_row.settlement_operation_id=requested_operation
       AND attempt_row.settlement_input_digest=requested_digest THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',true,
            'value',jsonb_build_object('protocol','forest.memory-embedding-abandoned/1',
                'claim_id',requested_claim,'attempt_id',requested_attempt,
                'state','NOT_SENT'));
    END IF;
    SELECT * INTO claim_row FROM __forest_module_schema__.embedding_claims
    WHERE bot_id=requested_bot AND claim_id=requested_claim FOR UPDATE;
    IF attempt_row.state<>'PREPARED' OR claim_row IS NULL
       OR claim_row.state NOT IN ('CLAIMED','EXPIRED') THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object('code','MEMORY_EMBEDDING_ATTEMPT_NOT_ABANDONABLE',
                'message','Only a prepared embedding attempt can be abandoned','status',409));
    END IF;
    UPDATE __forest_module_schema__.embedding_batch_attempts
    SET state='NOT_SENT',settlement_operation_id=requested_operation,
        settlement_input_digest=requested_digest,
        error_code=body->>'reason_code',settled_at=clock_timestamp()
    WHERE bot_id=requested_bot AND attempt_id=requested_attempt;
    UPDATE __forest_module_schema__.embedding_facets f
    SET state='PENDING',current_attempt_id=NULL,
        next_attempt_at=clock_timestamp()+interval '1 minute',
        updated_at=clock_timestamp()
    FROM __forest_module_schema__.embedding_claim_items ci
    WHERE ci.bot_id=requested_bot AND ci.claim_id=requested_claim
      AND f.bot_id=ci.bot_id AND f.facet_id=ci.facet_id
      AND f.state='PREPARED' AND f.current_attempt_id=requested_attempt;
    GET DIAGNOSTICS updated_count = ROW_COUNT;
    IF updated_count<>claim_row.item_count THEN
        RAISE EXCEPTION 'Embedding claim membership changed under lock' USING ERRCODE='55000';
    END IF;
    IF claim_row.state='CLAIMED' THEN
        UPDATE __forest_module_schema__.embedding_claims
        SET state='EXPIRED' WHERE bot_id=requested_bot AND claim_id=requested_claim;
    END IF;
    RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',true,
        'value',jsonb_build_object('protocol','forest.memory-embedding-abandoned/1',
            'claim_id',requested_claim,'attempt_id',requested_attempt,
            'state','NOT_SENT'));
END;
$$;

CREATE FUNCTION memory_observe_embedding_attempt(context jsonb, envelope jsonb)
RETURNS jsonb LANGUAGE plpgsql VOLATILE SET search_path = pg_catalog AS $$
DECLARE
    body jsonb := envelope->'input';
    effect_value jsonb := envelope#>'{prior,effect}';
    effect_observation jsonb := effect_value->'observation';
    effect_evidence jsonb := effect_value->'evidence_payload';
    effect_parents jsonb := effect_value->'evidence_parents';
    budget_value jsonb := envelope#>'{prior,budget}';
    result_value jsonb := body->'result';
    vectors_value jsonb := body->'vectors';
    requested_bot text := context->>'bot_id';
    requested_operation text := context->>'operation_id';
    requested_digest text := context->>'participant_input_digest';
    requested_claim text := body->>'claim_id';
    requested_attempt text := body->>'attempt_id';
    requested_effect text := body->>'effect_operation_id';
    observed_state text := body->>'observed_state';
    budget_state text;
    attempt_state text;
    claim_row __forest_module_schema__.embedding_claims%ROWTYPE;
    attempt_row __forest_module_schema__.embedding_batch_attempts%ROWTYPE;
    facet_row record;
    vector_value jsonb;
    seen_count integer := 0;
BEGIN
    IF jsonb_typeof(effect_parents) IS DISTINCT FROM 'array'
       OR jsonb_typeof(vectors_value) IS DISTINCT FROM 'array' THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object('code','MEMORY_EMBEDDING_OBSERVATION_INVALID',
                'message','Embedding observation evidence is invalid','status',422));
    END IF;
    budget_state := CASE observed_state WHEN 'SUCCEEDED' THEN 'CHARGED'
        WHEN 'NOT_APPLIED' THEN 'RELEASED' WHEN 'UNKNOWN' THEN 'UNKNOWN' END;
    attempt_state := CASE observed_state WHEN 'SUCCEEDED' THEN 'SUCCEEDED'
        WHEN 'NOT_APPLIED' THEN 'NOT_SENT' WHEN 'UNKNOWN' THEN 'UNKNOWN' END;
    IF envelope->>'protocol' IS DISTINCT FROM 'forest.uow-step/1'
       OR envelope#>>'{caller,component_id}' IS DISTINCT FROM 'memory.graph'
       OR body->>'protocol' IS DISTINCT FROM 'forest.memory-embedding-observe/1'
       OR requested_bot IS NULL OR requested_operation IS NULL
       OR requested_digest !~ '^sha256:[a-f0-9]{64}$'
       OR requested_claim IS NULL OR requested_attempt IS NULL
       OR requested_effect IS NULL OR budget_state IS NULL
       OR effect_value->>'protocol' IS DISTINCT FROM 'forest.effect-command-observation/1'
       OR effect_observation->>'attempt_id' IS DISTINCT FROM requested_attempt
       OR effect_observation->>'observation_operation_id' IS DISTINCT FROM
          body->>'observation_operation_id'
       OR effect_observation->>'observed_status' IS DISTINCT FROM observed_state
       OR effect_observation->>'resulting_state' IS DISTINCT FROM observed_state
       OR effect_observation->>'evidence_kind' IS DISTINCT FROM 'executor'
       OR effect_observation->>'accepted' IS DISTINCT FROM 'true'
       OR effect_evidence->>'owner_component_id' IS DISTINCT FROM 'kernel.effect'
       OR effect_evidence->>'owner_kind' IS DISTINCT FROM 'effect.evidence'
       OR effect_evidence->>'owner_id' IS DISTINCT FROM body->>'observation_operation_id'
       OR effect_evidence->>'digest' IS DISTINCT FROM body->>'evidence_digest'
       OR budget_value->>'protocol' IS DISTINCT FROM 'forest.memory-embedding-budget/1'
       OR budget_value->>'reservation_id' IS DISTINCT FROM body->>'reservation_id'
       OR budget_value->>'state' IS DISTINCT FROM budget_state
       OR budget_value->>'charged_tokens' IS DISTINCT FROM body->>'charged_tokens'
       OR (observed_state='SUCCEEDED' AND (
           result_value IS NULL OR result_value='null'::jsonb
           OR result_value->>'owner_component_id' IS DISTINCT FROM 'embedding.multimodal'
           OR result_value->>'owner_kind' IS DISTINCT FROM 'embedding.result'
           OR result_value->>'owner_id' IS DISTINCT FROM requested_attempt
           OR jsonb_array_length(effect_parents)<>1
           OR effect_parents->0 IS DISTINCT FROM result_value
       ))
       OR (observed_state<>'SUCCEEDED' AND (
           result_value IS DISTINCT FROM 'null'::jsonb
           OR jsonb_array_length(effect_parents)<>0
           OR jsonb_array_length(vectors_value)<>0
           OR body->>'charged_tokens' IS NOT NULL
       )) THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object('code','MEMORY_EMBEDDING_OBSERVATION_INVALID',
                'message','Embedding observation evidence is invalid','status',422));
    END IF;
    SELECT * INTO attempt_row FROM __forest_module_schema__.embedding_batch_attempts
    WHERE bot_id=requested_bot AND attempt_id=requested_attempt FOR UPDATE;
    IF attempt_row IS NULL OR attempt_row.claim_id IS DISTINCT FROM requested_claim
       OR attempt_row.effect_operation_id IS DISTINCT FROM requested_effect
       OR attempt_row.reservation_id IS DISTINCT FROM body->>'reservation_id' THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object('code','MEMORY_EMBEDDING_ATTEMPT_CONFLICT',
                'message','Embedding attempt identity differs','status',409));
    END IF;
    IF attempt_row.state=attempt_state
       AND attempt_row.settlement_operation_id=requested_operation
       AND attempt_row.settlement_input_digest=requested_digest THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',true,
            'value',jsonb_build_object('protocol','forest.memory-embedding-observed/1',
                'claim_id',requested_claim,'attempt_id',requested_attempt,
                'state',attempt_state));
    END IF;
    SELECT * INTO claim_row FROM __forest_module_schema__.embedding_claims
    WHERE bot_id=requested_bot AND claim_id=requested_claim FOR UPDATE;
    IF attempt_row.state<>'DISPATCHED' OR claim_row IS NULL
       OR claim_row.state<>'CLAIMED' THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object('code','MEMORY_EMBEDDING_OBSERVATION_STALE',
                'message','Embedding observation boundary is closed','status',409));
    END IF;
    IF observed_state='SUCCEEDED' AND
       (jsonb_array_length(vectors_value)<>claim_row.item_count
        OR body->>'charged_tokens' !~ '^[0-9]+$') THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object('code','MEMORY_EMBEDDING_VECTOR_SET_INVALID',
                'message','Embedding vector set differs from the claim','status',422));
    END IF;
    FOR facet_row IN
        SELECT ci.item_index,ci.facet_id,f.state,f.current_attempt_id
        FROM __forest_module_schema__.embedding_claim_items ci
        JOIN __forest_module_schema__.embedding_facets f
          ON f.bot_id=ci.bot_id AND f.facet_id=ci.facet_id
        WHERE ci.bot_id=requested_bot AND ci.claim_id=requested_claim
        ORDER BY ci.item_index FOR UPDATE OF f
    LOOP
        seen_count := seen_count+1;
        IF facet_row.state NOT IN ('DISPATCHED','SUPPRESSED')
           OR (facet_row.state='DISPATCHED' AND
               facet_row.current_attempt_id IS DISTINCT FROM requested_attempt) THEN
            RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
                'error',jsonb_build_object('code','MEMORY_EMBEDDING_FACET_CHANGED',
                    'message','Embedding facet changed before observation','status',409));
        END IF;
        IF observed_state='SUCCEEDED' THEN
            vector_value := vectors_value->facet_row.item_index;
            IF vector_value->>'facet_id' IS DISTINCT FROM facet_row.facet_id
               OR vector_value->>'index' IS DISTINCT FROM facet_row.item_index::text
               OR vector_value->>'vector_digest' !~ '^sha256:[a-f0-9]{64}$' THEN
                RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
                    'error',jsonb_build_object('code','MEMORY_EMBEDDING_VECTOR_SET_INVALID',
                        'message','Embedding vector set differs from the claim','status',422));
            END IF;
            IF facet_row.state='DISPATCHED' THEN
                UPDATE __forest_module_schema__.embedding_facets
                SET state='EMBEDDED',vector_digest=vector_value->>'vector_digest',
                    result_payload_id=result_value->>'payload_id',
                    result_payload_digest=result_value->>'digest',
                    updated_at=clock_timestamp()
                WHERE bot_id=requested_bot AND facet_id=facet_row.facet_id;
            END IF;
        ELSIF facet_row.state='DISPATCHED' THEN
            UPDATE __forest_module_schema__.embedding_facets
            SET state=CASE observed_state WHEN 'NOT_APPLIED' THEN 'PENDING'
                    ELSE 'UNKNOWN' END,
                current_attempt_id=CASE observed_state WHEN 'NOT_APPLIED' THEN NULL
                    ELSE current_attempt_id END,
                next_attempt_at=CASE observed_state WHEN 'NOT_APPLIED' THEN
                    clock_timestamp()+interval '1 minute' ELSE next_attempt_at END,
                updated_at=clock_timestamp()
            WHERE bot_id=requested_bot AND facet_id=facet_row.facet_id;
        END IF;
    END LOOP;
    IF seen_count<>claim_row.item_count THEN
        RAISE EXCEPTION 'Embedding claim membership changed under lock' USING ERRCODE='55000';
    END IF;
    UPDATE __forest_module_schema__.embedding_batch_attempts
    SET state=attempt_state,settlement_operation_id=requested_operation,
        settlement_input_digest=requested_digest,
        result_payload_id=CASE WHEN observed_state='SUCCEEDED' THEN
            result_value->>'payload_id' ELSE NULL END,
        result_payload_digest=CASE WHEN observed_state='SUCCEEDED' THEN
            result_value->>'digest' ELSE NULL END,
        error_code=body->>'error_code',settled_at=clock_timestamp()
    WHERE bot_id=requested_bot AND attempt_id=requested_attempt;
    UPDATE __forest_module_schema__.embedding_claims
    SET state=CASE observed_state WHEN 'NOT_APPLIED' THEN 'EXPIRED' ELSE 'SETTLED' END,
        settled_at=CASE observed_state WHEN 'NOT_APPLIED' THEN NULL
            ELSE clock_timestamp() END
    WHERE bot_id=requested_bot AND claim_id=requested_claim;
    RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',true,
        'value',jsonb_build_object('protocol','forest.memory-embedding-observed/1',
            'claim_id',requested_claim,'attempt_id',requested_attempt,
            'state',attempt_state));
END;
$$;

CREATE FUNCTION memory_reserve_embedding_budget(context jsonb, envelope jsonb)
RETURNS jsonb LANGUAGE plpgsql VOLATILE SET search_path = pg_catalog AS $$
DECLARE
    body jsonb := envelope->'input';
    requested_bot text := context->>'bot_id';
    requested_operation text := context->>'operation_id';
    requested_digest text := context->>'participant_input_digest';
    reservation_value text := body->>'reservation_id';
    request_limit_value integer;
    token_limit_value bigint;
    reserved_tokens_value bigint;
    budget_day_value date;
    existing __forest_module_schema__.embedding_budget_reservations%ROWTYPE;
    day_row __forest_module_schema__.embedding_budget_days%ROWTYPE;
BEGIN
    IF envelope->>'protocol' IS DISTINCT FROM 'forest.uow-step/1'
       OR envelope#>>'{caller,component_id}' IS DISTINCT FROM 'memory.graph'
       OR body->>'protocol' IS DISTINCT FROM 'forest.memory-embedding-budget-reserve/1'
       OR requested_bot IS NULL OR requested_operation IS NULL
       OR requested_digest !~ '^sha256:[a-f0-9]{64}$'
       OR reservation_value IS NULL OR octet_length(reservation_value) NOT BETWEEN 1 AND 256
       OR octet_length(body->>'batch_ref') NOT BETWEEN 1 AND 256
       OR body->>'policy_revision' IS DISTINCT FROM 'daily-cost-cap/1'
       OR body->>'request_limit' !~ '^[1-9][0-9]*$'
       OR body->>'token_limit' !~ '^[1-9][0-9]*$'
       OR body->>'reserved_tokens' !~ '^[1-9][0-9]*$' THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object('code','MEMORY_EMBEDDING_BUDGET_INPUT_INVALID',
                'message','Embedding budget reservation is invalid','status',422));
    END IF;
    request_limit_value := (body->>'request_limit')::integer;
    token_limit_value := (body->>'token_limit')::bigint;
    reserved_tokens_value := (body->>'reserved_tokens')::bigint;
    IF request_limit_value NOT BETWEEN 1 AND 1000000
       OR token_limit_value NOT BETWEEN 1 AND 1000000000000
       OR reserved_tokens_value NOT BETWEEN 1 AND LEAST(token_limit_value,1000000000) THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object('code','MEMORY_EMBEDDING_BUDGET_INPUT_INVALID',
                'message','Embedding budget limits are invalid','status',422));
    END IF;
    PERFORM pg_advisory_xact_lock(hashtextextended(
        'forest.memory.graph/budget-operation/'||requested_bot||'/'||requested_operation,0));
    PERFORM pg_advisory_xact_lock(hashtextextended(
        'forest.memory.graph/budget-batch/'||requested_bot||'/'||(body->>'batch_ref'),0));
    PERFORM pg_advisory_xact_lock(hashtextextended(
        'forest.memory.graph/budget-reservation/'||requested_bot||'/'||reservation_value,0));
    SELECT * INTO existing FROM __forest_module_schema__.embedding_budget_reservations
    WHERE bot_id=requested_bot AND
      (reservation_id=reservation_value OR operation_id=requested_operation
       OR batch_ref=(body->>'batch_ref')) FOR UPDATE;
    IF FOUND THEN
        IF existing.reservation_id IS DISTINCT FROM reservation_value
           OR existing.operation_id IS DISTINCT FROM requested_operation
           OR existing.batch_ref IS DISTINCT FROM body->>'batch_ref'
           OR existing.input_digest IS DISTINCT FROM requested_digest THEN
            RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
                'error',jsonb_build_object('code','MEMORY_EMBEDDING_BUDGET_CONFLICT',
                    'message','Embedding budget reservation identity differs','status',409));
        END IF;
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',true,
            'value',jsonb_build_object('protocol','forest.memory-embedding-budget/1',
                'reservation_id',existing.reservation_id,'state',existing.state,
                'budget_day',existing.budget_day,'reserved_tokens',existing.reserved_tokens,
                'charged_tokens',existing.charged_tokens));
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM __forest_module_schema__.memory_domains WHERE bot_id=requested_bot
    ) THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object('code','MEMORY_EMBEDDING_DOMAIN_UNBOUND',
                'message','Embedding domain is not bound','status',409));
    END IF;
    budget_day_value := (clock_timestamp() AT TIME ZONE 'UTC')::date;
    INSERT INTO __forest_module_schema__.embedding_budget_days(bot_id,budget_day)
    VALUES(requested_bot,budget_day_value) ON CONFLICT DO NOTHING;
    SELECT * INTO day_row FROM __forest_module_schema__.embedding_budget_days
    WHERE bot_id=requested_bot AND budget_day=budget_day_value FOR UPDATE;
    IF day_row.reserved_requests::bigint+day_row.settled_requests+
       day_row.unknown_requests+1 > request_limit_value
       OR day_row.reserved_tokens+day_row.charged_tokens+
          day_row.unknown_tokens+reserved_tokens_value > token_limit_value THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object('code','MEMORY_EMBEDDING_BUDGET_EXHAUSTED',
                'message','Daily embedding budget is exhausted','status',429));
    END IF;
    INSERT INTO __forest_module_schema__.embedding_budget_reservations(
        bot_id,reservation_id,operation_id,input_digest,batch_ref,policy_revision,
        budget_day,request_limit,token_limit,reserved_tokens,deadline_at)
    VALUES(requested_bot,reservation_value,requested_operation,requested_digest,
        body->>'batch_ref',body->>'policy_revision',budget_day_value,
        request_limit_value,token_limit_value,reserved_tokens_value,
        clock_timestamp()+interval '10 minutes');
    UPDATE __forest_module_schema__.embedding_budget_days
    SET reserved_requests=reserved_requests+1,
        reserved_tokens=reserved_tokens+reserved_tokens_value,
        revision=revision+1,updated_at=clock_timestamp()
    WHERE bot_id=requested_bot AND budget_day=budget_day_value;
    RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',true,
        'value',jsonb_build_object('protocol','forest.memory-embedding-budget/1',
            'reservation_id',reservation_value,'state','RESERVED',
            'budget_day',budget_day_value,'reserved_tokens',reserved_tokens_value,
            'charged_tokens',NULL));
EXCEPTION WHEN invalid_text_representation OR numeric_value_out_of_range
    OR not_null_violation OR check_violation THEN
    RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
        'error',jsonb_build_object('code','MEMORY_EMBEDDING_BUDGET_INPUT_INVALID',
            'message','Embedding budget reservation is invalid','status',422));
END;
$$;

CREATE FUNCTION memory_settle_embedding_budget(context jsonb, envelope jsonb)
RETURNS jsonb LANGUAGE plpgsql VOLATILE SET search_path = pg_catalog AS $$
DECLARE
    body jsonb := envelope->'input';
    requested_bot text := context->>'bot_id';
    requested_operation text := context->>'operation_id';
    requested_digest text := context->>'participant_input_digest';
    state_value text := body->>'state';
    charged_value bigint;
    reservation_row __forest_module_schema__.embedding_budget_reservations%ROWTYPE;
BEGIN
    IF envelope->>'protocol' IS DISTINCT FROM 'forest.uow-step/1'
       OR envelope#>>'{caller,component_id}' IS DISTINCT FROM 'memory.graph'
       OR body->>'protocol' IS DISTINCT FROM 'forest.memory-embedding-budget-settle/1'
       OR requested_bot IS NULL OR requested_operation IS NULL
       OR requested_digest !~ '^sha256:[a-f0-9]{64}$'
       OR octet_length(body->>'reservation_id') NOT BETWEEN 1 AND 256
       OR state_value NOT IN ('RELEASED','CHARGED','UNKNOWN')
       OR (state_value='CHARGED' AND body->>'charged_tokens' !~ '^[0-9]+$')
       OR (state_value<>'CHARGED' AND body->'charged_tokens'<>'null'::jsonb)
       OR (body->>'error_code' IS NOT NULL AND
           octet_length(body->>'error_code') NOT BETWEEN 1 AND 128) THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object('code','MEMORY_EMBEDDING_BUDGET_SETTLEMENT_INVALID',
                'message','Embedding budget settlement is invalid','status',422));
    END IF;
    charged_value := (body->>'charged_tokens')::bigint;
    SELECT * INTO reservation_row FROM __forest_module_schema__.embedding_budget_reservations
    WHERE bot_id=requested_bot AND reservation_id=body->>'reservation_id' FOR UPDATE;
    IF NOT FOUND THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object('code','MEMORY_EMBEDDING_BUDGET_MISSING',
                'message','Embedding budget reservation is missing','status',404));
    END IF;
    IF reservation_row.state<>'RESERVED' THEN
        IF reservation_row.settlement_operation_id IS DISTINCT FROM requested_operation
           OR reservation_row.settlement_input_digest IS DISTINCT FROM requested_digest THEN
            RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
                'error',jsonb_build_object('code','MEMORY_EMBEDDING_BUDGET_CONFLICT',
                    'message','Embedding budget was settled differently','status',409));
        END IF;
    ELSE
        PERFORM 1 FROM __forest_module_schema__.embedding_budget_days
        WHERE bot_id=requested_bot AND budget_day=reservation_row.budget_day FOR UPDATE;
        UPDATE __forest_module_schema__.embedding_budget_reservations
        SET state=state_value,settlement_operation_id=requested_operation,
            settlement_input_digest=requested_digest,charged_tokens=charged_value,
            error_code=body->>'error_code',settled_at=clock_timestamp()
        WHERE bot_id=requested_bot AND reservation_id=reservation_row.reservation_id
        RETURNING * INTO reservation_row;
        UPDATE __forest_module_schema__.embedding_budget_days
        SET reserved_requests=reserved_requests-1,
            reserved_tokens=reserved_tokens-reservation_row.reserved_tokens,
            settled_requests=settled_requests+CASE WHEN state_value='CHARGED' THEN 1 ELSE 0 END,
            charged_tokens=charged_tokens+CASE WHEN state_value='CHARGED' THEN charged_value ELSE 0 END,
            unknown_requests=unknown_requests+CASE WHEN state_value='UNKNOWN' THEN 1 ELSE 0 END,
            unknown_tokens=unknown_tokens+CASE WHEN state_value='UNKNOWN'
                THEN reservation_row.reserved_tokens ELSE 0 END,
            revision=revision+1,updated_at=clock_timestamp()
        WHERE bot_id=requested_bot AND budget_day=reservation_row.budget_day;
    END IF;
    RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',true,
        'value',jsonb_build_object('protocol','forest.memory-embedding-budget/1',
            'reservation_id',reservation_row.reservation_id,'state',reservation_row.state,
            'budget_day',reservation_row.budget_day,
            'reserved_tokens',reservation_row.reserved_tokens,
            'charged_tokens',reservation_row.charged_tokens));
EXCEPTION WHEN invalid_text_representation OR numeric_value_out_of_range
    OR not_null_violation OR check_violation THEN
    RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
        'error',jsonb_build_object('code','MEMORY_EMBEDDING_BUDGET_SETTLEMENT_INVALID',
            'message','Embedding budget settlement is invalid','status',422));
END;
$$;

CREATE FUNCTION memory_capture_source(context jsonb, envelope jsonb)
RETURNS jsonb
LANGUAGE plpgsql
VOLATILE
SET search_path = pg_catalog
AS $$
DECLARE
    body jsonb := envelope->'input'; capture jsonb := body->'capture'; prior jsonb := envelope->'prior';
    requested_bot text := context->>'bot_id'; requested_operation text := context->>'operation_id';
    requested_digest text := context->>'participant_input_digest'; caller_component text := envelope#>>'{caller,component_id}';
    source_value jsonb := capture->'identity'; occurrence_value jsonb := capture->'occurrence';
    author_value jsonb := capture->'author'; parts_value jsonb := capture->'parts';
    source_id_value text := source_value->>'source_id'; unit_id_value text := capture->>'source_unit_id';
    occurrence_id_value text := occurrence_value->>'occurrence_id'; conversation_step text := body->>'conversation_step_id';
    epoch_step text := body->>'deletion_epoch_step_id'; conversation_value jsonb; epoch_value jsonb;
    ingress_seq_value bigint; deletion_epoch_value bigint; identity_digest_value text;
    source_input_digest text; manifest_digest_value text; modality_value text; source_order_value bigint;
    source_row record; identity_row record; unit_row record; occurrence_row record; operation_row record; pending_row record;
    archive_pending_row record;
    part jsonb; duplicate_value boolean := false; eligibility_value text := 'REGISTERED';
BEGIN
    IF envelope->>'protocol' IS DISTINCT FROM 'forest.uow-step/1'
       OR caller_component NOT IN ('adapter.onebot','memory.graph')
       OR requested_operation IS NULL OR requested_digest !~ '^sha256:[a-f0-9]{64}$'
       OR jsonb_typeof(source_value) IS DISTINCT FROM 'object'
       OR jsonb_typeof(occurrence_value) IS DISTINCT FROM 'object'
       OR jsonb_typeof(author_value) IS DISTINCT FROM 'object'
       OR jsonb_typeof(parts_value) IS DISTINCT FROM 'array'
       OR jsonb_array_length(parts_value) NOT BETWEEN 1 AND 4096 THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,'error',jsonb_build_object(
            'code','MEMORY_CAPTURE_INPUT_INVALID','message','Memory capture envelope is invalid','status',422));
    END IF;
    epoch_value := prior->epoch_step;
    IF epoch_step IS NULL OR epoch_value->>'protocol' IS DISTINCT FROM 'forest.payload-epoch/1'
       OR epoch_value->>'deletion_epoch' !~ '^[0-9]+$' THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,'error',jsonb_build_object(
            'code','MEMORY_CAPTURE_AUTHORITY_MISSING','message','Payload deletion epoch is unavailable','status',503));
    END IF;
    deletion_epoch_value := (epoch_value->>'deletion_epoch')::bigint;
    PERFORM pg_advisory_xact_lock(hashtextextended(
        'forest.memory.graph/eligibility/' || requested_bot || '/' ||
        (occurrence_value->>'conversation_ref') || '/' || (occurrence_value->>'event_ref'),0
    ));
    IF EXISTS (
        SELECT 1 FROM __forest_module_schema__.unavailable_source_events
        WHERE bot_id=requested_bot AND conversation_ref=occurrence_value->>'conversation_ref'
          AND event_ref=occurrence_value->>'event_ref'
    ) THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object('code','MEMORY_SOURCE_UNAVAILABLE',
                'message','Unavailable source cannot later be captured','status',409));
    END IF;
    IF conversation_step IS NOT NULL THEN
        conversation_value := prior->conversation_step;
        IF conversation_value->>'conversation_id' IS DISTINCT FROM occurrence_value->>'conversation_ref'
           OR conversation_value->>'event_id' IS DISTINCT FROM occurrence_value->>'event_ref'
           OR conversation_value->>'ingress_seq' !~ '^[0-9]+$' THEN
            RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,'error',jsonb_build_object(
                'code','MEMORY_CAPTURE_CONVERSATION_CONFLICT','message','Conversation admission differs from Memory occurrence','status',409));
        END IF;
        ingress_seq_value := (conversation_value->>'ingress_seq')::bigint;
    ELSE
        IF caller_component<>'memory.graph' OR occurrence_value->>'ingress_seq' !~ '^[0-9]+$' THEN
            RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,'error',jsonb_build_object(
                'code','MEMORY_CAPTURE_CONVERSATION_CONFLICT','message','Memory capture has no admitted ingress sequence','status',409));
        END IF;
        ingress_seq_value := (occurrence_value->>'ingress_seq')::bigint;
    END IF;
    PERFORM pg_advisory_xact_lock(hashtextextended(
        'forest.memory.graph/capture/'||requested_bot||'/'||requested_operation,0));
    SELECT * INTO operation_row FROM __forest_module_schema__.capture_operations
    WHERE bot_id=requested_bot AND operation_id=requested_operation;
    IF FOUND THEN
        IF operation_row.input_digest IS DISTINCT FROM requested_digest THEN
            RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,'error',jsonb_build_object(
                'code','MEMORY_CAPTURE_CONFLICT','message','Memory capture operation input differs','status',409));
        END IF;
        SELECT eligibility_state INTO eligibility_value FROM __forest_module_schema__.source_units
        WHERE bot_id=requested_bot AND source_unit_id=operation_row.source_unit_id;
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',true,'value',jsonb_build_object(
            'protocol','forest.memory-source-captured/1','source_id',source_id_value,
            'source_unit_id',operation_row.source_unit_id,'occurrence_id',operation_row.occurrence_id,
            'duplicate_source',operation_row.duplicate_source,'eligibility_state',eligibility_value));
    END IF;
    identity_digest_value := 'sha256:'||encode(sha256(convert_to(source_value::text,'UTF8')),'hex');
    source_input_digest := identity_digest_value;
    PERFORM pg_advisory_xact_lock(hashtextextended(
        'forest.memory.graph/source/'||requested_bot||'/'||source_id_value,0));
    PERFORM pg_advisory_xact_lock(hashtextextended(
        'forest.memory.graph/source-identity/'||requested_bot||'/'||identity_digest_value,0));
    SELECT * INTO source_row FROM __forest_module_schema__.memory_sources
    WHERE bot_id=requested_bot AND source_id=source_id_value FOR UPDATE;
    SELECT * INTO identity_row FROM __forest_module_schema__.memory_sources
    WHERE bot_id=requested_bot AND identity_digest=identity_digest_value FOR UPDATE;
    IF source_row IS NOT NULL AND identity_row IS NOT NULL
       AND source_row.source_id IS DISTINCT FROM identity_row.source_id THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,'error',jsonb_build_object(
            'code','MEMORY_SOURCE_IDENTITY_CONFLICT','message','Memory source id and identity refer to different sources','status',409));
    END IF;
    IF source_row IS NULL THEN
        source_row := identity_row;
    END IF;
    IF source_row IS NOT NULL THEN
        IF source_row.source_id IS DISTINCT FROM source_id_value
           OR source_row.source_kind IS DISTINCT FROM source_value->>'source_kind'
           OR source_row.platform IS DISTINCT FROM source_value->>'platform'
           OR source_row.realm_id IS DISTINCT FROM source_value->>'realm_id'
           OR source_row.external_group_id IS DISTINCT FROM source_value->>'external_group_id'
           OR source_row.identity_incarnation IS DISTINCT FROM (source_value->>'identity_incarnation')::bigint
           OR source_row.trunk_id IS DISTINCT FROM source_value->>'trunk_id' THEN
            RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,'error',jsonb_build_object(
                'code','MEMORY_SOURCE_IDENTITY_CONFLICT','message','Memory source identity differs','status',409));
        END IF;
        duplicate_value := true;
    ELSE
        INSERT INTO __forest_module_schema__.memory_sources(
            bot_id,source_id,registration_operation_id,input_digest,source_kind,platform,realm_id,
            external_group_id,identity_incarnation,identity_digest,trunk_id)
        VALUES(requested_bot,source_id_value,
            'register:'||encode(sha256(convert_to(source_id_value,'UTF8')),'hex'),source_input_digest,
            source_value->>'source_kind',source_value->>'platform',source_value->>'realm_id',
            source_value->>'external_group_id',(source_value->>'identity_incarnation')::bigint,
            identity_digest_value,source_value->>'trunk_id');
        IF source_value->>'trunk_id' IS NOT NULL THEN
            INSERT INTO __forest_module_schema__.memory_trunks(bot_id,trunk_id,source_id)
            VALUES(requested_bot,source_value->>'trunk_id',source_id_value);
        END IF;
        INSERT INTO __forest_module_schema__.source_clocks(bot_id,source_id)
        VALUES(requested_bot,source_id_value);
    END IF;
    PERFORM pg_advisory_xact_lock(hashtextextended(
        'forest.memory.graph/unit/'||requested_bot||'/'||unit_id_value,0));
    SELECT * INTO unit_row FROM __forest_module_schema__.source_units
    WHERE bot_id=requested_bot AND source_unit_id=unit_id_value FOR UPDATE;
    IF FOUND THEN
        IF unit_row.input_digest IS DISTINCT FROM requested_digest
           OR unit_row.source_id IS DISTINCT FROM source_id_value
           OR unit_row.source_revision IS DISTINCT FROM (capture->>'source_revision')::bigint THEN
            RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,'error',jsonb_build_object(
                'code','MEMORY_SOURCE_UNIT_CONFLICT','message','Memory source unit identity differs','status',409));
        END IF;
    ELSE
        UPDATE __forest_module_schema__.source_clocks SET next_source_order_seq=next_source_order_seq+1,
            updated_at=clock_timestamp() WHERE bot_id=requested_bot AND source_id=source_id_value
        RETURNING next_source_order_seq-1 INTO source_order_value;
        manifest_digest_value := 'sha256:'||encode(sha256(convert_to(parts_value::text,'UTF8')),'hex');
        SELECT CASE WHEN bool_and(value->>'part_kind'='TEXT') THEN 'TEXT'
                    WHEN bool_and(value->>'part_kind'='IMAGE') THEN 'IMAGE' ELSE 'MIXED' END
        INTO modality_value FROM jsonb_array_elements(parts_value);
        INSERT INTO __forest_module_schema__.source_units(
            bot_id,source_unit_id,capture_operation_id,input_digest,source_id,trunk_id,
            source_order_seq,source_revision,unit_kind,modality,part_count,content_manifest_digest,
            author_subject_ref,author_payload_id,author_payload_digest,author_owner_component_id,
            author_owner_kind,author_owner_id,accepted_deletion_epoch,occurred_at)
        VALUES(requested_bot,unit_id_value,requested_operation,requested_digest,source_id_value,
            source_value->>'trunk_id',source_order_value,(capture->>'source_revision')::bigint,
            capture->>'unit_kind',modality_value,jsonb_array_length(parts_value),manifest_digest_value,
            capture->>'author_subject_ref',author_value->>'payload_id',author_value->>'digest',
            author_value->>'owner_component_id',author_value->>'owner_kind',author_value->>'owner_id',
            deletion_epoch_value,(capture->>'occurred_at')::timestamptz);
        FOR part IN SELECT value FROM jsonb_array_elements(parts_value) LOOP
            INSERT INTO __forest_module_schema__.source_parts(
                bot_id,source_unit_id,part_id,part_index,part_kind,media_type,payload_id,payload_digest,
                payload_owner_component_id,payload_owner_kind,payload_owner_id,width,height,byte_length)
            VALUES(requested_bot,unit_id_value,part->>'part_id',(part->>'part_index')::integer,
                part->>'part_kind',part->>'media_type',part#>>'{payload,payload_id}',part#>>'{payload,digest}',
                part#>>'{payload,owner_component_id}',part#>>'{payload,owner_kind}',part#>>'{payload,owner_id}',
                (part->>'width')::integer,(part->>'height')::integer,(part->>'byte_length')::bigint);
        END LOOP;
    END IF;
    PERFORM pg_advisory_xact_lock(hashtextextended(
        'forest.memory.graph/occurrence/'||requested_bot||'/'||occurrence_id_value,0));
    PERFORM pg_advisory_xact_lock(hashtextextended(
        'forest.memory.graph/event/'||requested_bot||'/'||(occurrence_value->>'binding_ref')||'/'||
        (occurrence_value->>'event_ref'),0));
    SELECT * INTO occurrence_row FROM __forest_module_schema__.source_occurrences
    WHERE bot_id=requested_bot AND (occurrence_id=occurrence_id_value OR
        (binding_ref=occurrence_value->>'binding_ref' AND event_ref=occurrence_value->>'event_ref'))
    FOR UPDATE;
    IF FOUND THEN
        IF occurrence_row.occurrence_id IS DISTINCT FROM occurrence_id_value
           OR occurrence_row.source_unit_id IS DISTINCT FROM unit_id_value
           OR occurrence_row.conversation_ref IS DISTINCT FROM occurrence_value->>'conversation_ref'
           OR occurrence_row.ingress_seq IS DISTINCT FROM ingress_seq_value THEN
            RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,'error',jsonb_build_object(
                'code','MEMORY_OCCURRENCE_CONFLICT','message','Memory occurrence identity differs','status',409));
        END IF;
    ELSE
        INSERT INTO __forest_module_schema__.source_occurrences(
            bot_id,occurrence_id,source_unit_id,binding_ref,conversation_ref,event_ref,ingress_seq,observed_at)
        VALUES(requested_bot,occurrence_id_value,unit_id_value,occurrence_value->>'binding_ref',
            occurrence_value->>'conversation_ref',occurrence_value->>'event_ref',ingress_seq_value,
            (occurrence_value->>'observed_at')::timestamptz);
    END IF;
    SELECT * INTO pending_row FROM __forest_module_schema__.pending_input_events
    WHERE bot_id=requested_bot AND conversation_ref=occurrence_value->>'conversation_ref'
      AND event_ref=occurrence_value->>'event_ref' FOR UPDATE;
    IF FOUND AND pending_row.state='PENDING_CAPTURE' THEN
        UPDATE __forest_module_schema__.source_occurrences SET eligibility_state='ADMITTED',
            input_batch_ref=pending_row.input_batch_ref,context_revision=pending_row.context_revision,
            qualified_at=clock_timestamp() WHERE bot_id=requested_bot AND occurrence_id=occurrence_id_value;
        UPDATE __forest_module_schema__.pending_input_events SET state='RESOLVED',
            source_unit_id=unit_id_value,occurrence_id=occurrence_id_value,resolved_at=clock_timestamp()
        WHERE bot_id=requested_bot AND conversation_ref=occurrence_value->>'conversation_ref'
          AND event_ref=occurrence_value->>'event_ref';
    END IF;
    SELECT * INTO archive_pending_row FROM __forest_module_schema__.pending_archive_events
    WHERE bot_id=requested_bot AND conversation_ref=occurrence_value->>'conversation_ref'
      AND event_ref=occurrence_value->>'event_ref' FOR UPDATE;
    IF archive_pending_row IS NOT NULL AND archive_pending_row.state='PENDING_CAPTURE' THEN
        SELECT * INTO occurrence_row FROM __forest_module_schema__.source_occurrences
        WHERE bot_id=requested_bot AND occurrence_id=occurrence_id_value FOR UPDATE;
        IF occurrence_row.eligibility_state='SUPPRESSED' OR
           (occurrence_row.input_batch_ref IS NOT NULL AND
            occurrence_row.input_batch_ref IS DISTINCT FROM archive_pending_row.input_batch_ref) OR
           (occurrence_row.context_revision IS NOT NULL AND
            occurrence_row.context_revision IS DISTINCT FROM archive_pending_row.context_revision) THEN
            RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
                'error',jsonb_build_object('code','MEMORY_ARCHIVE_CONFLICT',
                    'message','Archived occurrence differs from captured source','status',409));
        END IF;
        UPDATE __forest_module_schema__.source_occurrences
        SET eligibility_state='ELIGIBLE',input_batch_ref=archive_pending_row.input_batch_ref,
            context_revision=archive_pending_row.context_revision,qualified_at=clock_timestamp()
        WHERE bot_id=requested_bot AND occurrence_id=occurrence_id_value
          AND eligibility_state IN ('REGISTERED','ADMITTED');
        UPDATE __forest_module_schema__.source_units
        SET eligibility_state='ELIGIBLE',eligibility_revision=eligibility_revision+1,
            eligible_at=clock_timestamp(),updated_at=clock_timestamp()
        WHERE bot_id=requested_bot AND source_unit_id=unit_id_value
          AND eligibility_state='REGISTERED';
        SELECT * INTO unit_row FROM __forest_module_schema__.source_units
        WHERE bot_id=requested_bot AND source_unit_id=unit_id_value;
        INSERT INTO __forest_module_schema__.archive_qualification_operations(
            bot_id,operation_id,input_digest,compaction_ref,conversation_ref,event_ref,
            source_unit_id,occurrence_id,resulting_eligibility_revision)
        VALUES(requested_bot,archive_pending_row.operation_id,
            archive_pending_row.input_digest,archive_pending_row.compaction_ref,
            archive_pending_row.conversation_ref,archive_pending_row.event_ref,
            unit_id_value,occurrence_id_value,unit_row.eligibility_revision);
        IF unit_row.eligibility_state='ELIGIBLE' THEN
            INSERT INTO __forest_module_schema__.facet_preparation_work(
                bot_id,source_unit_id)
            VALUES(requested_bot,unit_id_value)
            ON CONFLICT (bot_id,source_unit_id) DO NOTHING;
        END IF;
        UPDATE __forest_module_schema__.pending_archive_events
        SET state='RESOLVED',source_unit_id=unit_id_value,
            occurrence_id=occurrence_id_value,resolved_at=clock_timestamp()
        WHERE bot_id=requested_bot AND conversation_ref=occurrence_value->>'conversation_ref'
          AND event_ref=occurrence_value->>'event_ref';
    END IF;
    SELECT eligibility_state INTO eligibility_value FROM __forest_module_schema__.source_units
    WHERE bot_id=requested_bot AND source_unit_id=unit_id_value;
    INSERT INTO __forest_module_schema__.capture_operations(
        bot_id,operation_id,input_digest,source_unit_id,occurrence_id,duplicate_source)
    VALUES(requested_bot,requested_operation,requested_digest,unit_id_value,occurrence_id_value,duplicate_value);
    RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',true,'value',jsonb_build_object(
        'protocol','forest.memory-source-captured/1','source_id',source_id_value,
        'source_unit_id',unit_id_value,'occurrence_id',occurrence_id_value,
        'duplicate_source',duplicate_value,'eligibility_state',eligibility_value));
EXCEPTION WHEN invalid_text_representation OR datetime_field_overflow OR not_null_violation OR check_violation THEN
    RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,'error',jsonb_build_object(
        'code','MEMORY_CAPTURE_INPUT_INVALID','message','Memory capture payload is invalid','status',422));
END;
$$;

CREATE FUNCTION memory_prepare_source_forget(context jsonb, envelope jsonb)
RETURNS jsonb LANGUAGE plpgsql VOLATILE SET search_path = pg_catalog AS $$
DECLARE
    body jsonb := envelope->'input';
    requested_bot text := context->>'bot_id';
    unit_row record;
    suppression_row record;
BEGIN
    IF envelope->>'protocol' IS DISTINCT FROM 'forest.uow-step/1'
       OR envelope#>>'{caller,component_id}' IS DISTINCT FROM 'memory.graph'
       OR body->>'operation_id' IS NULL
       OR octet_length(body->>'operation_id') NOT BETWEEN 1 AND 256
       OR body->>'source_unit_id' IS NULL
       OR octet_length(body->>'source_unit_id') NOT BETWEEN 1 AND 256 THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object('code','MEMORY_FORGET_INPUT_INVALID',
                'message','Memory forget request is invalid','status',422));
    END IF;
    SELECT * INTO unit_row FROM __forest_module_schema__.source_units
    WHERE bot_id=requested_bot AND source_unit_id=body->>'source_unit_id' FOR UPDATE;
    IF unit_row IS NULL THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object('code','MEMORY_SOURCE_NOT_FOUND',
                'message','Memory source unit does not exist','status',404));
    END IF;
    SELECT * INTO suppression_row FROM __forest_module_schema__.source_suppressions
    WHERE bot_id=requested_bot AND source_unit_id=unit_row.source_unit_id;
    IF unit_row.eligibility_state='SUPPRESSED' AND
       (suppression_row IS NULL OR suppression_row.operation_id IS DISTINCT FROM
        body->>'operation_id') THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object('code','MEMORY_SOURCE_SUPPRESSED',
                'message','Memory source unit was already forgotten','status',409));
    END IF;
    RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',true,
        'value',jsonb_build_object('protocol','forest.payload-forget-plan/1',
            'anchor_payload_id',unit_row.author_payload_id));
END;
$$;

CREATE FUNCTION memory_apply_source_forget(context jsonb, envelope jsonb)
RETURNS jsonb LANGUAGE plpgsql VOLATILE SET search_path = pg_catalog AS $$
DECLARE
    body jsonb := envelope->'input';
    prior jsonb := envelope->'prior';
    prepared jsonb := prior->(body->>'prepare_step_id');
    forgotten jsonb := prior->(body->>'forget_step_id');
    requested_bot text := context->>'bot_id';
    requested_digest text := context->>'participant_input_digest';
    operation_row record;
    unit_row record;
    selected_ids text[];
    affected_ids text[] := ARRAY[]::text[];
    deletion_epoch_value bigint;
    suppressed_count integer := 0;
    deleted_refs integer := 0;
    inserted_refs integer;
    retracted_count integer := 0;
    result_value jsonb;
BEGIN
    IF envelope->>'protocol' IS DISTINCT FROM 'forest.uow-step/1'
       OR envelope#>>'{caller,component_id}' IS DISTINCT FROM 'memory.graph'
       OR body->>'operation_id' IS NULL
       OR octet_length(body->>'operation_id') NOT BETWEEN 1 AND 256
       OR body->>'source_unit_id' IS NULL
       OR octet_length(body->>'source_unit_id') NOT BETWEEN 1 AND 256
       OR requested_digest !~ '^sha256:[a-f0-9]{64}$'
       OR prepared->>'protocol' IS DISTINCT FROM 'forest.payload-forget-plan/1'
       OR forgotten->>'protocol' IS DISTINCT FROM 'forest.payload-forgotten/1'
       OR forgotten->>'operation_id' IS DISTINCT FROM body->>'operation_id'
       OR forgotten->>'selection_digest' !~ '^sha256:[a-f0-9]{64}$'
       OR forgotten->>'deletion_epoch' !~ '^[1-9][0-9]*$'
       OR jsonb_typeof(forgotten->'selected_payload_ids') IS DISTINCT FROM 'array'
       OR jsonb_array_length(forgotten->'selected_payload_ids') > 100000 THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object('code','MEMORY_FORGET_AUTHORITY_INVALID',
                'message','Payload forget receipt is invalid','status',422));
    END IF;
    SELECT array_agg(value ORDER BY value) INTO selected_ids
    FROM jsonb_array_elements_text(forgotten->'selected_payload_ids');
    IF selected_ids IS NULL OR prepared->>'anchor_payload_id' <> ALL(selected_ids) THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object('code','MEMORY_FORGET_ANCHOR_MISSING',
                'message','Payload forget selection omitted the Memory anchor','status',409));
    END IF;
    deletion_epoch_value := (forgotten->>'deletion_epoch')::bigint;
    IF NOT EXISTS (
        SELECT 1 FROM __forest_module_schema__.source_units
        WHERE bot_id=requested_bot AND source_unit_id=body->>'source_unit_id'
          AND author_payload_id=prepared->>'anchor_payload_id'
    ) THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object('code','MEMORY_FORGET_ANCHOR_MISSING',
                'message','Payload forget anchor differs from the Memory source','status',409));
    END IF;
    SELECT * INTO operation_row FROM __forest_module_schema__.source_forget_operations
    WHERE bot_id=requested_bot AND operation_id=body->>'operation_id';
    IF operation_row IS NOT NULL THEN
        IF operation_row.input_digest IS DISTINCT FROM requested_digest
           OR operation_row.source_unit_id IS DISTINCT FROM body->>'source_unit_id'
           OR operation_row.selection_digest IS DISTINCT FROM forgotten->>'selection_digest'
           OR operation_row.deletion_epoch IS DISTINCT FROM deletion_epoch_value THEN
            RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
                'error',jsonb_build_object('code','MEMORY_FORGET_OPERATION_CONFLICT',
                    'message','Memory forget operation differs','status',409));
        END IF;
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',true,
            'value',operation_row.result_payload);
    END IF;
    FOR unit_row IN
        SELECT u.* FROM __forest_module_schema__.source_units u
        WHERE u.bot_id=requested_bot AND (
            u.author_payload_id=ANY(selected_ids) OR EXISTS (
                SELECT 1 FROM __forest_module_schema__.source_parts p
                WHERE p.bot_id=u.bot_id AND p.source_unit_id=u.source_unit_id
                  AND p.payload_id=ANY(selected_ids)))
        ORDER BY u.source_unit_id FOR UPDATE
    LOOP
        IF unit_row.eligibility_state='SUPPRESSED' THEN
            CONTINUE;
        END IF;
        affected_ids := array_append(affected_ids,unit_row.source_unit_id);
        INSERT INTO __forest_module_schema__.source_suppressions(
            bot_id,operation_id,input_digest,source_unit_id,deletion_epoch,reason_code)
        VALUES(requested_bot,body->>'operation_id',requested_digest,
            unit_row.source_unit_id,deletion_epoch_value,'PUBLIC_FORGET');
        UPDATE __forest_module_schema__.source_units
        SET eligibility_state='SUPPRESSED',eligibility_revision=eligibility_revision+1,
            updated_at=clock_timestamp()
        WHERE bot_id=requested_bot AND source_unit_id=unit_row.source_unit_id;
        UPDATE __forest_module_schema__.source_occurrences
        SET eligibility_state='SUPPRESSED',input_batch_ref=NULL,context_revision=NULL
        WHERE bot_id=requested_bot AND source_unit_id=unit_row.source_unit_id
          AND eligibility_state<>'SUPPRESSED';
        INSERT INTO __forest_module_schema__.vector_index_deletions(
            bot_id,index_ref_id,space_id,source_unit_id,deletion_epoch)
        SELECT bot_id,facet_id,space_id,source_unit_id,deletion_epoch_value
        FROM __forest_module_schema__.embedding_facets
        WHERE bot_id=requested_bot AND source_unit_id=unit_row.source_unit_id
        ON CONFLICT (bot_id,index_ref_id) DO NOTHING;
        GET DIAGNOSTICS inserted_refs = ROW_COUNT;
        deleted_refs := deleted_refs + inserted_refs;
        UPDATE __forest_module_schema__.embedding_facets
        SET state='SUPPRESSED',index_ref_id=NULL,vector_digest=NULL,
            result_payload_id=NULL,result_payload_digest=NULL,published_at=NULL,
            updated_at=clock_timestamp()
        WHERE bot_id=requested_bot AND source_unit_id=unit_row.source_unit_id
          AND state<>'SUPPRESSED';
        UPDATE __forest_module_schema__.facet_preparation_work
        SET state='SUPPRESSED',claim_token=NULL,lease_deadline=NULL,
            prepared_facet_count=NULL,updated_at=clock_timestamp()
        WHERE bot_id=requested_bot AND source_unit_id=unit_row.source_unit_id
          AND state NOT IN ('SUPPRESSED','CLAIMED');
        suppressed_count := suppressed_count+1;
    END LOOP;
    IF (body->>'source_unit_id') <> ALL(affected_ids) THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object('code','MEMORY_FORGET_ANCHOR_MISSING',
                'message','Memory source unit was not selected by Payload','status',409));
    END IF;
    UPDATE __forest_module_schema__.relation_contributions c SET active=false
    WHERE c.bot_id=requested_bot AND c.active AND (
        (c.left_kind='SOURCE_UNIT' AND c.left_id=ANY(affected_ids)) OR
        (c.right_kind='SOURCE_UNIT' AND c.right_id=ANY(affected_ids)) OR EXISTS (
            SELECT 1 FROM __forest_module_schema__.relation_evidence e
            WHERE e.bot_id=c.bot_id AND e.contribution_id=c.contribution_id
              AND e.source_unit_id=ANY(affected_ids)));
    GET DIAGNOSTICS retracted_count = ROW_COUNT;
    result_value := jsonb_build_object('protocol','forest.memory-source-forgotten/1',
        'source_unit_id',body->>'source_unit_id','deletion_epoch',deletion_epoch_value,
        'suppressed_source_units',suppressed_count,'vector_deletions',deleted_refs,
        'retracted_relations',retracted_count);
    INSERT INTO __forest_module_schema__.source_forget_operations(
        bot_id,operation_id,input_digest,source_unit_id,selection_digest,
        deletion_epoch,result_payload)
    VALUES(requested_bot,body->>'operation_id',requested_digest,
        body->>'source_unit_id',forgotten->>'selection_digest',
        deletion_epoch_value,result_value);
    RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',true,
        'value',result_value);
END;
$$;

CREATE FUNCTION memory_mark_source_unavailable(context jsonb, envelope jsonb)
RETURNS jsonb LANGUAGE plpgsql VOLATILE SET search_path = pg_catalog AS $$
DECLARE
    body jsonb := envelope->'input'; requested_bot text := context->>'bot_id';
    requested_operation text := context->>'operation_id'; requested_digest text := context->>'participant_input_digest';
    existing record; pending_row record;
BEGIN
    IF envelope->>'protocol' IS DISTINCT FROM 'forest.uow-step/1'
       OR envelope#>>'{caller,component_id}' IS DISTINCT FROM 'memory.graph'
       OR requested_operation IS NULL OR requested_digest !~ '^sha256:[a-f0-9]{64}$' THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,'error',jsonb_build_object(
            'code','MEMORY_UNAVAILABLE_INPUT_INVALID','message','Memory unavailable envelope is invalid','status',422));
    END IF;
    PERFORM pg_advisory_xact_lock(hashtextextended(
        'forest.memory.graph/eligibility/' || requested_bot || '/' ||
        (body->>'conversation_ref') || '/' || (body->>'event_ref'),0
    ));
    IF EXISTS (
        SELECT 1 FROM __forest_module_schema__.source_occurrences
        WHERE bot_id=requested_bot AND conversation_ref=body->>'conversation_ref'
          AND event_ref=body->>'event_ref'
    ) THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object('code','MEMORY_SOURCE_ALREADY_CAPTURED',
                'message','Captured source cannot become unavailable','status',409));
    END IF;
    INSERT INTO __forest_module_schema__.unavailable_source_events(
        bot_id,conversation_ref,event_ref,operation_id,input_digest,error_code)
    VALUES(requested_bot,body->>'conversation_ref',body->>'event_ref',requested_operation,
        requested_digest,body->>'error_code') ON CONFLICT DO NOTHING;
    SELECT * INTO existing FROM __forest_module_schema__.unavailable_source_events
    WHERE bot_id=requested_bot AND conversation_ref=body->>'conversation_ref' AND event_ref=body->>'event_ref';
    IF existing.operation_id IS DISTINCT FROM requested_operation
       OR existing.input_digest IS DISTINCT FROM requested_digest THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,'error',jsonb_build_object(
            'code','MEMORY_UNAVAILABLE_CONFLICT','message','Memory unavailable identity differs','status',409));
    END IF;
    SELECT * INTO pending_row FROM __forest_module_schema__.pending_input_events
    WHERE bot_id=requested_bot AND conversation_ref=body->>'conversation_ref' AND event_ref=body->>'event_ref' FOR UPDATE;
    IF FOUND AND pending_row.state='PENDING_CAPTURE' THEN
        UPDATE __forest_module_schema__.pending_input_events SET state='UNAVAILABLE',
            error_code=body->>'error_code',resolved_at=clock_timestamp()
        WHERE bot_id=requested_bot AND conversation_ref=body->>'conversation_ref' AND event_ref=body->>'event_ref';
    END IF;
    UPDATE __forest_module_schema__.pending_archive_events
    SET state='UNAVAILABLE',error_code=body->>'error_code',resolved_at=clock_timestamp()
    WHERE bot_id=requested_bot AND conversation_ref=body->>'conversation_ref'
      AND event_ref=body->>'event_ref' AND state='PENDING_CAPTURE';
    RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',true,'value',jsonb_build_object(
        'protocol','forest.memory-source-unavailable/1','conversation_ref',body->>'conversation_ref',
        'event_ref',body->>'event_ref','error_code',body->>'error_code'));
END;
$$;

CREATE FUNCTION memory_qualify_input_batch(context jsonb, envelope jsonb)
RETURNS jsonb
LANGUAGE plpgsql
VOLATILE
SET search_path = pg_catalog
AS $$
DECLARE
    body jsonb := envelope->'input';
    prior jsonb := envelope->'prior';
    requested_bot text := context->>'bot_id';
    requested_operation text := context->>'operation_id';
    requested_digest text := context->>'participant_input_digest';
    selection_step text := body->>'selection_step_id';
    epoch_step text := body->>'deletion_epoch_step_id';
    selection jsonb;
    epoch_value jsonb;
    requested_batch text;
    requested_conversation text;
    requested_context_revision bigint;
    requested_deletion_epoch bigint;
    event_refs jsonb;
    event_ref_value text;
    occurrence_row record;
    unit_row record;
    pending_row record;
    unavailable_row record;
    replay_row record;
    item_operation_id text;
    item_digest text;
    qualified_count integer := 0;
    pending_count integer := 0;
BEGIN
    IF envelope->>'protocol' IS DISTINCT FROM 'forest.uow-step/1'
       OR envelope#>>'{caller,component_id}' IS DISTINCT FROM 'agent.runtime'
       OR body IS NULL OR prior IS NULL
       OR selection_step IS NULL OR epoch_step IS NULL
       OR requested_operation IS NULL
       OR requested_digest !~ '^sha256:[a-f0-9]{64}$' THEN
        RETURN jsonb_build_object(
            'protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object(
                'code','MEMORY_INPUT_BATCH_INVALID',
                'message','Memory InputBatch envelope is invalid','status',422
            )
        );
    END IF;
    selection := prior->selection_step;
    epoch_value := prior->epoch_step;
    IF selection IS NULL OR epoch_value IS NULL
       OR epoch_value->>'protocol' IS DISTINCT FROM 'forest.payload-epoch/1'
       OR epoch_value->>'deletion_epoch' IS NULL
       OR epoch_value->>'deletion_epoch' !~ '^[0-9]+$' THEN
        RETURN jsonb_build_object(
            'protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object(
                'code','MEMORY_INPUT_AUTHORITY_MISSING',
                'message','Conversation selection or Payload epoch is unavailable','status',503
            )
        );
    END IF;
    requested_deletion_epoch := (epoch_value->>'deletion_epoch')::bigint;
    IF selection->>'status' IS DISTINCT FROM 'BATCH' THEN
        RETURN jsonb_build_object(
            'protocol','forest.domain-result/1','ok',true,
            'value',jsonb_build_object(
                'protocol','forest.memory-input-qualified/1',
                'input_batch_ref',NULL,
                'qualified_count',0,
                'pending_count',0,
                'deletion_epoch',requested_deletion_epoch
            )
        );
    END IF;
    requested_batch := selection->>'input_batch_id';
    requested_conversation := selection->>'conversation_id';
    event_refs := selection->'event_ids';
    IF requested_batch IS NULL OR requested_conversation IS NULL
       OR selection->>'context_revision' IS NULL
       OR selection->>'context_revision' !~ '^[0-9]+$'
       OR jsonb_typeof(event_refs) IS DISTINCT FROM 'array'
       OR jsonb_array_length(event_refs) > 100000 THEN
        RETURN jsonb_build_object(
            'protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object(
                'code','MEMORY_INPUT_BATCH_INVALID',
                'message','Conversation selection is not one bounded InputBatch','status',422
            )
        );
    END IF;
    requested_context_revision := (selection->>'context_revision')::bigint;
    IF requested_context_revision < 1 THEN
        RETURN jsonb_build_object(
            'protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object(
                'code','MEMORY_INPUT_BATCH_INVALID',
                'message','Conversation selection revision is invalid','status',422
            )
        );
    END IF;
    IF (SELECT count(*) FROM jsonb_array_elements_text(event_refs))
       <> (SELECT count(DISTINCT value) FROM jsonb_array_elements_text(event_refs)) THEN
        RETURN jsonb_build_object(
            'protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object(
                'code','MEMORY_INPUT_BATCH_INVALID',
                'message','InputBatch event identities repeat','status',422
            )
        );
    END IF;
    -- Reserve the whole InputBatch in a stable order before touching rows.
    FOR event_ref_value IN
        SELECT value FROM jsonb_array_elements_text(event_refs) ORDER BY value
    LOOP
        PERFORM pg_advisory_xact_lock(hashtextextended(
            'forest.memory.graph/eligibility/' || requested_bot || '/' ||
            requested_conversation || '/' || event_ref_value,0
        ));
    END LOOP;
    FOR event_ref_value IN
        SELECT value FROM jsonb_array_elements_text(event_refs) ORDER BY value
    LOOP
        IF octet_length(event_ref_value) NOT BETWEEN 1 AND 256 THEN
            RETURN jsonb_build_object(
                'protocol','forest.domain-result/1','ok',false,
                'error',jsonb_build_object(
                    'code','MEMORY_INPUT_BATCH_INVALID',
                    'message','InputBatch event identity is invalid','status',422
                )
            );
        END IF;
        SELECT * INTO occurrence_row
        FROM __forest_module_schema__.source_occurrences
        WHERE bot_id=requested_bot AND conversation_ref=requested_conversation
          AND event_ref=event_ref_value
        FOR UPDATE;
        IF occurrence_row IS NULL THEN
            item_digest := 'sha256:' || encode(sha256(convert_to(jsonb_build_array(
                requested_digest,requested_batch,requested_conversation,event_ref_value,
                requested_context_revision,requested_deletion_epoch
            )::text,'UTF8')),'hex');
            SELECT * INTO unavailable_row
            FROM __forest_module_schema__.unavailable_source_events
            WHERE bot_id=requested_bot AND conversation_ref=requested_conversation
              AND event_ref=event_ref_value;
            INSERT INTO __forest_module_schema__.pending_input_events(
                bot_id,conversation_ref,event_ref,operation_id,input_digest,
                input_batch_ref,context_revision,accepted_deletion_epoch,state,error_code,resolved_at
            ) VALUES (
                requested_bot,requested_conversation,event_ref_value,requested_operation,
                item_digest,requested_batch,requested_context_revision,
                requested_deletion_epoch,
                CASE WHEN unavailable_row IS NULL THEN 'PENDING_CAPTURE' ELSE 'UNAVAILABLE' END,
                unavailable_row.error_code,
                CASE WHEN unavailable_row IS NULL THEN NULL ELSE clock_timestamp() END
            ) ON CONFLICT (bot_id,conversation_ref,event_ref) DO NOTHING;
            SELECT * INTO pending_row
            FROM __forest_module_schema__.pending_input_events
            WHERE bot_id=requested_bot AND conversation_ref=requested_conversation
              AND event_ref=event_ref_value;
            IF pending_row.input_digest IS DISTINCT FROM item_digest THEN
                RETURN jsonb_build_object(
                    'protocol','forest.domain-result/1','ok',false,
                    'error',jsonb_build_object(
                        'code','MEMORY_ELIGIBILITY_CONFLICT',
                        'message','Pending Memory eligibility differs','status',409
                    )
                );
            END IF;
            IF pending_row.state='PENDING_CAPTURE' THEN
                pending_count := pending_count + 1;
            ELSE
                qualified_count := qualified_count + 1;
            END IF;
            CONTINUE;
        END IF;
        SELECT * INTO unit_row
        FROM __forest_module_schema__.source_units
        WHERE bot_id=requested_bot AND source_unit_id=occurrence_row.source_unit_id
        FOR UPDATE;
        IF unit_row IS NULL THEN
            RETURN jsonb_build_object(
                'protocol','forest.domain-result/1','ok',false,
                'error',jsonb_build_object(
                    'code','MEMORY_SOURCE_NOT_FOUND',
                    'message','Memory source unit is unavailable','status',404
                )
            );
        END IF;
        IF unit_row.eligibility_state='SUPPRESSED'
           OR occurrence_row.eligibility_state='SUPPRESSED' THEN
            RETURN jsonb_build_object(
                'protocol','forest.domain-result/1','ok',false,
                'error',jsonb_build_object(
                    'code','MEMORY_SOURCE_SUPPRESSED',
                    'message','Memory source unit was removed','status',409
                )
            );
        END IF;
        IF occurrence_row.eligibility_state IN ('ADMITTED','ELIGIBLE') AND (
            occurrence_row.input_batch_ref IS DISTINCT FROM requested_batch
            OR occurrence_row.context_revision IS DISTINCT FROM requested_context_revision
        ) THEN
            RETURN jsonb_build_object(
                'protocol','forest.domain-result/1','ok',false,
                'error',jsonb_build_object(
                    'code','MEMORY_ELIGIBILITY_CONFLICT',
                    'message','Memory occurrence qualification differs','status',409
                )
            );
        END IF;
        item_operation_id := 'eligibility:' || encode(sha256(convert_to(jsonb_build_array(
            requested_bot,requested_operation,occurrence_row.occurrence_id
        )::text,'UTF8')),'hex');
        item_digest := 'sha256:' || encode(sha256(convert_to(jsonb_build_array(
            occurrence_row.source_unit_id,occurrence_row.occurrence_id,requested_batch,
            requested_context_revision,requested_deletion_epoch
        )::text,'UTF8')),'hex');
        SELECT * INTO replay_row
        FROM __forest_module_schema__.eligibility_operations
        WHERE bot_id=requested_bot AND operation_id=item_operation_id;
        IF replay_row IS NOT NULL AND replay_row.input_digest IS DISTINCT FROM item_digest THEN
            RETURN jsonb_build_object(
                'protocol','forest.domain-result/1','ok',false,
                'error',jsonb_build_object(
                    'code','MEMORY_ELIGIBILITY_CONFLICT',
                    'message','Memory eligibility replay differs','status',409
                )
            );
        END IF;
        IF occurrence_row.eligibility_state='REGISTERED' THEN
            UPDATE __forest_module_schema__.source_occurrences
            SET eligibility_state='ADMITTED',input_batch_ref=requested_batch,
                context_revision=requested_context_revision,qualified_at=clock_timestamp()
            WHERE bot_id=requested_bot AND occurrence_id=occurrence_row.occurrence_id;
        END IF;
        IF replay_row IS NULL THEN
            INSERT INTO __forest_module_schema__.eligibility_operations(
                bot_id,operation_id,input_digest,source_unit_id,occurrence_id,
                input_batch_ref,context_revision,resulting_eligibility_revision
            ) VALUES (
                requested_bot,item_operation_id,item_digest,occurrence_row.source_unit_id,
                occurrence_row.occurrence_id,requested_batch,requested_context_revision,
                unit_row.eligibility_revision
            );
        END IF;
        qualified_count := qualified_count + 1;
    END LOOP;
    RETURN jsonb_build_object(
        'protocol','forest.domain-result/1','ok',true,
        'value',jsonb_build_object(
            'protocol','forest.memory-input-qualified/1',
            'input_batch_ref',requested_batch,
            'qualified_count',qualified_count,
            'pending_count',pending_count,
            'deletion_epoch',requested_deletion_epoch
        )
    );
END;
$$;

CREATE FUNCTION memory_qualify_archive(context jsonb, envelope jsonb)
RETURNS jsonb LANGUAGE plpgsql VOLATILE SET search_path = pg_catalog AS $$
DECLARE
    prior jsonb := envelope->'prior';
    prepared jsonb := prior->'prepare';
    committed jsonb := prior->'commit';
    epoch_value jsonb := prior->'payload_epoch';
    requested_bot text := context->>'bot_id';
    requested_operation text := context->>'operation_id';
    requested_digest text := context->>'participant_input_digest';
    archive_value jsonb;
    units jsonb;
    unit_value jsonb;
    expected_events jsonb;
    event_ref_value text;
    conversation_ref_value text;
    compaction_ref_value text;
    batch_ref_value text;
    context_revision_value bigint;
    deletion_epoch_value bigint;
    item_operation text;
    item_digest text;
    occurrence_row record;
    unit_row record;
    pending_row record;
    unavailable_row record;
    replay_row record;
BEGIN
    IF envelope->>'protocol' IS DISTINCT FROM 'forest.uow-step/1'
       OR envelope#>>'{caller,component_id}' IS DISTINCT FROM 'conversation.scheduler'
       OR requested_operation IS NULL OR requested_digest !~ '^sha256:[a-f0-9]{64}$'
       OR prepared IS NULL OR committed IS NULL
       OR epoch_value->>'protocol' IS DISTINCT FROM 'forest.payload-epoch/1'
       OR epoch_value->>'deletion_epoch' !~ '^[0-9]+$' THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object('code','MEMORY_ARCHIVE_INPUT_INVALID',
                'message','Memory archive authority is incomplete','status',422));
    END IF;
    IF prepared->>'status'='SKIP' THEN
        IF committed ? 'memory_archive' THEN
            RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
                'error',jsonb_build_object('code','MEMORY_ARCHIVE_CONFLICT',
                    'message','Skipped compaction claimed Memory archive','status',409));
        END IF;
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',true,
            'value',committed);
    END IF;
    archive_value := committed->'memory_archive';
    units := prepared#>'{commit,units}';
    conversation_ref_value := prepared#>>'{commit,conversation_id}';
    compaction_ref_value := prepared#>>'{commit,compaction_id}';
    deletion_epoch_value := (epoch_value->>'deletion_epoch')::bigint;
    IF prepared->>'status' IS DISTINCT FROM 'WRITE'
       OR jsonb_typeof(units) IS DISTINCT FROM 'array'
       OR jsonb_typeof(archive_value) IS DISTINCT FROM 'object'
       OR archive_value->>'compaction_ref' IS DISTINCT FROM compaction_ref_value
       OR committed->>'conversation_id' IS DISTINCT FROM conversation_ref_value
       OR jsonb_array_length(units) IS DISTINCT FROM
          (prepared#>>'{commit,archived_unit_count}')::bigint THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object('code','MEMORY_ARCHIVE_CONFLICT',
                'message','Conversation archive membership differs','status',409));
    END IF;
    SELECT COALESCE(jsonb_agg(item->>'event_id' ORDER BY ordinality)
        FILTER (WHERE item->>'event_id' IS NOT NULL),'[]'::jsonb)
    INTO expected_events
    FROM jsonb_array_elements(units) WITH ORDINALITY AS u(item,ordinality);
    IF archive_value->'event_refs' IS DISTINCT FROM expected_events THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object('code','MEMORY_ARCHIVE_CONFLICT',
            'message','Conversation archive event set differs','status',409));
    END IF;
    -- Capture, InputBatch selection and archive may overlap. Acquire every
    -- event fence in the same order before locking any occurrence or unit row.
    FOR event_ref_value IN
        SELECT value FROM jsonb_array_elements_text(expected_events) ORDER BY value
    LOOP
        PERFORM pg_advisory_xact_lock(hashtextextended(
            'forest.memory.graph/eligibility/' || requested_bot || '/' ||
            conversation_ref_value || '/' || event_ref_value,0
        ));
    END LOOP;
    FOR unit_value IN SELECT value FROM jsonb_array_elements(units) LOOP
        event_ref_value := unit_value->>'event_id';
        IF event_ref_value IS NULL THEN CONTINUE; END IF;
        batch_ref_value := unit_value->>'input_batch_id';
        IF batch_ref_value IS NULL OR unit_value->>'context_revision' !~ '^[1-9][0-9]*$'
           OR octet_length(event_ref_value) NOT BETWEEN 1 AND 256 THEN
            RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
                'error',jsonb_build_object('code','MEMORY_ARCHIVE_INPUT_INVALID',
                    'message','Archived event has no stable InputBatch','status',422));
        END IF;
        context_revision_value := (unit_value->>'context_revision')::bigint;
        item_operation := 'archive:' || encode(sha256(convert_to(jsonb_build_array(
            requested_bot,compaction_ref_value,event_ref_value
        )::text,'UTF8')),'hex');
        item_digest := 'sha256:' || encode(sha256(convert_to(jsonb_build_array(
            requested_digest,compaction_ref_value,conversation_ref_value,event_ref_value,
            batch_ref_value,context_revision_value,deletion_epoch_value
        )::text,'UTF8')),'hex');
        SELECT * INTO replay_row FROM __forest_module_schema__.archive_qualification_operations
        WHERE bot_id=requested_bot AND compaction_ref=compaction_ref_value
          AND event_ref=event_ref_value;
        IF replay_row IS NOT NULL THEN
            IF replay_row.operation_id IS DISTINCT FROM item_operation
               OR replay_row.input_digest IS DISTINCT FROM item_digest THEN
                RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
                    'error',jsonb_build_object('code','MEMORY_ARCHIVE_CONFLICT',
                        'message','Archive replay differs','status',409));
            END IF;
            CONTINUE;
        END IF;
        SELECT * INTO occurrence_row FROM __forest_module_schema__.source_occurrences
        WHERE bot_id=requested_bot AND conversation_ref=conversation_ref_value
          AND event_ref=event_ref_value FOR UPDATE;
        IF occurrence_row IS NULL THEN
            SELECT * INTO pending_row FROM __forest_module_schema__.pending_input_events
            WHERE bot_id=requested_bot AND conversation_ref=conversation_ref_value
              AND event_ref=event_ref_value FOR UPDATE;
            IF pending_row IS NOT NULL AND (
                pending_row.input_batch_ref IS DISTINCT FROM batch_ref_value OR
                pending_row.context_revision IS DISTINCT FROM context_revision_value
            ) THEN
                RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
                    'error',jsonb_build_object('code','MEMORY_ARCHIVE_CONFLICT',
                        'message','Pending input and archive disagree','status',409));
            END IF;
            SELECT * INTO unavailable_row FROM __forest_module_schema__.unavailable_source_events
            WHERE bot_id=requested_bot AND conversation_ref=conversation_ref_value
              AND event_ref=event_ref_value;
            INSERT INTO __forest_module_schema__.pending_archive_events(
                bot_id,conversation_ref,event_ref,operation_id,input_digest,
                compaction_ref,input_batch_ref,context_revision,accepted_deletion_epoch,
                state,error_code,resolved_at)
            VALUES(requested_bot,conversation_ref_value,event_ref_value,
                item_operation,item_digest,compaction_ref_value,batch_ref_value,
                context_revision_value,deletion_epoch_value,
                CASE WHEN unavailable_row IS NULL THEN 'PENDING_CAPTURE' ELSE 'UNAVAILABLE' END,
                unavailable_row.error_code,
                CASE WHEN unavailable_row IS NULL THEN NULL ELSE clock_timestamp() END)
            ON CONFLICT (bot_id,conversation_ref,event_ref) DO NOTHING;
            SELECT * INTO pending_row FROM __forest_module_schema__.pending_archive_events
            WHERE bot_id=requested_bot AND conversation_ref=conversation_ref_value
              AND event_ref=event_ref_value;
            IF pending_row.input_digest IS DISTINCT FROM item_digest
               OR pending_row.compaction_ref IS DISTINCT FROM compaction_ref_value THEN
                RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
                    'error',jsonb_build_object('code','MEMORY_ARCHIVE_CONFLICT',
                        'message','Pending archive identity differs','status',409));
            END IF;
            CONTINUE;
        END IF;
        IF occurrence_row.eligibility_state='SUPPRESSED'
           OR (occurrence_row.input_batch_ref IS NOT NULL AND
               occurrence_row.input_batch_ref IS DISTINCT FROM batch_ref_value)
           OR (occurrence_row.context_revision IS NOT NULL AND
               occurrence_row.context_revision IS DISTINCT FROM context_revision_value) THEN
            RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
                'error',jsonb_build_object('code','MEMORY_ARCHIVE_CONFLICT',
                    'message','Archived occurrence differs from Memory capture','status',409));
        END IF;
        UPDATE __forest_module_schema__.source_occurrences
        SET eligibility_state='ELIGIBLE',input_batch_ref=batch_ref_value,
            context_revision=context_revision_value,qualified_at=clock_timestamp()
        WHERE bot_id=requested_bot AND occurrence_id=occurrence_row.occurrence_id
          AND eligibility_state IN ('REGISTERED','ADMITTED');
        UPDATE __forest_module_schema__.source_units
        SET eligibility_state='ELIGIBLE',eligibility_revision=eligibility_revision+1,
            eligible_at=clock_timestamp(),updated_at=clock_timestamp()
        WHERE bot_id=requested_bot AND source_unit_id=occurrence_row.source_unit_id
          AND eligibility_state='REGISTERED';
        SELECT * INTO unit_row FROM __forest_module_schema__.source_units
        WHERE bot_id=requested_bot AND source_unit_id=occurrence_row.source_unit_id;
        IF unit_row IS NULL OR unit_row.eligibility_state<>'ELIGIBLE' THEN
            RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
                'error',jsonb_build_object('code','MEMORY_ARCHIVE_CONFLICT',
                    'message','Archived source unit is unavailable','status',409));
        END IF;
        INSERT INTO __forest_module_schema__.archive_qualification_operations(
            bot_id,operation_id,input_digest,compaction_ref,conversation_ref,event_ref,
            source_unit_id,occurrence_id,resulting_eligibility_revision)
        VALUES(requested_bot,item_operation,item_digest,compaction_ref_value,
            conversation_ref_value,event_ref_value,occurrence_row.source_unit_id,
            occurrence_row.occurrence_id,unit_row.eligibility_revision);
        INSERT INTO __forest_module_schema__.facet_preparation_work(bot_id,source_unit_id)
        VALUES(requested_bot,occurrence_row.source_unit_id)
        ON CONFLICT (bot_id,source_unit_id) DO NOTHING;
    END LOOP;
    RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',true,
        'value',committed);
END;
$$;

CREATE FUNCTION memory_read_facet_source(context jsonb, input jsonb)
RETURNS jsonb LANGUAGE plpgsql STABLE SET search_path = pg_catalog AS $$
DECLARE
    requested_bot text := context->>'bot_id';
    requested_source text := input->>'source_unit_id';
    after_index integer := (input->>'after_part_index')::integer;
    requested_limit integer := (input->>'limit')::integer;
    unit_row __forest_module_schema__.source_units%ROWTYPE;
    parts_value jsonb;
    last_index integer;
    has_more_value boolean := false;
BEGIN
    IF requested_source IS NULL OR octet_length(requested_source) NOT BETWEEN 1 AND 256
       OR after_index IS NULL OR after_index NOT BETWEEN -1 AND 4095
       OR requested_limit IS NULL OR requested_limit NOT BETWEEN 1 AND 64 THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object('code','MEMORY_FACET_SOURCE_INPUT_INVALID',
                'message','Facet source page request is invalid','status',422));
    END IF;
    SELECT u.* INTO unit_row FROM __forest_module_schema__.source_units u
    JOIN __forest_module_schema__.memory_sources s
      ON s.bot_id=u.bot_id AND s.source_id=u.source_id AND s.status='ACTIVE'
    WHERE u.bot_id=requested_bot AND u.source_unit_id=requested_source
      AND u.eligibility_state='ELIGIBLE';
    IF unit_row IS NULL THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',true,
            'value',jsonb_build_object('status','UNAVAILABLE','source_revision',NULL,
                'parts','[]'::jsonb,'next_part_index',NULL,'has_more',false));
    END IF;
    SELECT COALESCE(jsonb_agg(page.part ORDER BY page.part_index),'[]'::jsonb),
           max(page.part_index)
    INTO parts_value,last_index
    FROM (
        SELECT p.part_index,jsonb_build_object(
            'part_id',p.part_id,'part_index',p.part_index,'part_kind',p.part_kind,
            'media_type',p.media_type,'byte_length',p.byte_length,
            'payload',jsonb_build_object(
                'payload_id',p.payload_id,'digest',p.payload_digest,
                'owner_component_id',p.payload_owner_component_id,
                'owner_kind',p.payload_owner_kind,'owner_id',p.payload_owner_id
            )
        ) AS part
        FROM __forest_module_schema__.source_parts p
        WHERE p.bot_id=requested_bot AND p.source_unit_id=requested_source
          AND p.part_index>after_index
        ORDER BY p.part_index LIMIT requested_limit
    ) page;
    IF last_index IS NOT NULL THEN
        SELECT EXISTS(
            SELECT 1 FROM __forest_module_schema__.source_parts p
            WHERE p.bot_id=requested_bot AND p.source_unit_id=requested_source
              AND p.part_index>last_index
        ) INTO has_more_value;
    END IF;
    RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',true,
        'value',jsonb_build_object('status','READY',
            'source_revision',unit_row.source_revision,
            'parts',parts_value,'next_part_index',last_index,
            'has_more',has_more_value));
EXCEPTION WHEN invalid_text_representation OR numeric_value_out_of_range THEN
    RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
        'error',jsonb_build_object('code','MEMORY_FACET_SOURCE_INPUT_INVALID',
            'message','Facet source page request is invalid','status',422));
END;
$$;

CREATE FUNCTION memory_claim_embedding_batch(context jsonb, input jsonb)
RETURNS jsonb LANGUAGE plpgsql VOLATILE SET search_path = pg_catalog AS $$
DECLARE
    requested_bot text := context->>'bot_id';
    requested_token text := input->>'claim_token';
    requested_lease integer := (input->>'lease_seconds')::integer;
    prior_claim __forest_module_schema__.embedding_claims%ROWTYPE;
    domain_space text;
    deletion_epoch_value bigint;
    selected_facets jsonb;
    selected_purpose text;
    claim_value jsonb;
BEGIN
    IF input->>'protocol' IS DISTINCT FROM 'forest.owned-work-claim-request/1'
       OR requested_bot IS NULL OR requested_token IS NULL
       OR octet_length(requested_token) NOT BETWEEN 1 AND 256
       OR requested_lease IS NULL OR requested_lease NOT BETWEEN 3 AND 86400 THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object('code','MEMORY_EMBEDDING_CLAIM_INVALID',
                'message','Embedding batch claim is invalid','status',422));
    END IF;
    -- The exclusion join cannot see a concurrent transaction's new claim.
    -- Serialize batch selection for this Bot before reading candidates.
    PERFORM pg_catalog.pg_advisory_xact_lock(pg_catalog.hashtextextended(
        requested_bot||'/memory.embedding.claim',0));
    SELECT * INTO prior_claim FROM __forest_module_schema__.embedding_claims
    WHERE bot_id=requested_bot AND claim_id=requested_token;
    IF FOUND THEN
        IF prior_claim.lease_seconds IS DISTINCT FROM requested_lease THEN
            RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
                'error',jsonb_build_object('code','MEMORY_EMBEDDING_CLAIM_CONFLICT',
                    'message','Embedding claim token was reused differently','status',409));
        END IF;
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',true,
            'value',prior_claim.claim_payload);
    END IF;
    SELECT space_id INTO domain_space FROM __forest_module_schema__.memory_domains
    WHERE bot_id=requested_bot;
    IF NOT FOUND THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',true,'value',NULL);
    END IF;
    UPDATE __forest_module_schema__.embedding_claims c
    SET state='EXPIRED'
    WHERE c.bot_id=requested_bot AND c.state='CLAIMED'
      AND c.lease_deadline<=clock_timestamp()
      AND NOT EXISTS (
          SELECT 1 FROM __forest_module_schema__.embedding_batch_attempts a
          WHERE a.bot_id=c.bot_id AND a.claim_id=c.claim_id
      );
    WITH candidates AS MATERIALIZED (
        SELECT f.*,u.trunk_id,u.author_subject_ref,p.width,p.height,
               p.payload_owner_component_id,p.payload_owner_kind,p.payload_owner_id,
               o.conversation_ref
        FROM __forest_module_schema__.embedding_facets f
        JOIN __forest_module_schema__.source_units u
          ON u.bot_id=f.bot_id AND u.source_unit_id=f.source_unit_id
         AND u.source_revision=f.source_revision
         AND (u.eligibility_state='ELIGIBLE'
              OR (f.purpose='QUERY' AND u.eligibility_state='REGISTERED'))
        JOIN __forest_module_schema__.memory_sources s
          ON s.bot_id=u.bot_id AND s.source_id=u.source_id AND s.status='ACTIVE'
        JOIN __forest_module_schema__.source_parts p
          ON p.bot_id=f.bot_id AND p.source_unit_id=f.source_unit_id
         AND p.part_id=f.part_id AND p.payload_id=f.input_payload_id
         AND p.payload_digest=f.input_payload_digest
        JOIN LATERAL (
            SELECT occurrence.conversation_ref
            FROM __forest_module_schema__.source_occurrences occurrence
            WHERE occurrence.bot_id=f.bot_id
              AND occurrence.source_unit_id=f.source_unit_id
              AND ((f.purpose='DOCUMENT' AND occurrence.eligibility_state='ELIGIBLE')
                   OR (f.purpose='QUERY' AND
                       __forest_module_schema__.memory_query_occurrence_matches(
                           f.bot_id,f.query_id,f.source_unit_id,f.source_revision,
                           f.part_id,occurrence.occurrence_id)))
            ORDER BY occurrence.observed_at,occurrence.occurrence_id LIMIT 1
        ) o ON true
        WHERE f.bot_id=requested_bot
          AND f.state='PENDING' AND f.space_id=domain_space
          AND f.next_attempt_at<=clock_timestamp()
          AND NOT EXISTS (
              SELECT 1 FROM __forest_module_schema__.embedding_claim_items ci
              JOIN __forest_module_schema__.embedding_claims c
                ON c.bot_id=ci.bot_id AND c.claim_id=ci.claim_id
              WHERE ci.bot_id=f.bot_id AND ci.facet_id=f.facet_id
                AND c.state='CLAIMED' AND c.lease_deadline>clock_timestamp()
          )
        ORDER BY CASE f.purpose WHEN 'QUERY' THEN 0 ELSE 1 END,
                 f.created_at,f.facet_id LIMIT 32
        FOR UPDATE OF f SKIP LOCKED
    ), focus AS (
        SELECT purpose,query_id FROM candidates
        ORDER BY CASE purpose WHEN 'QUERY' THEN 0 ELSE 1 END,created_at,facet_id LIMIT 1
    ), ranked AS (
        SELECT candidates.*,
               sum(CASE WHEN modality='IMAGE' THEN input_byte_length ELSE 0 END)
                   OVER (ORDER BY candidates.created_at,candidates.facet_id) AS image_bytes
        FROM candidates JOIN focus ON candidates.purpose=focus.purpose
          AND candidates.query_id IS NOT DISTINCT FROM focus.query_id
    )
    SELECT COALESCE(jsonb_agg(jsonb_build_object(
        'facet_id',facet_id,'source_unit_id',source_unit_id,
        'source_revision',source_revision,'part_id',part_id,
        'trunk_id',trunk_id,'conversation_ref',conversation_ref,
        'author_ref',author_subject_ref,'modality',modality,
        'selector_start_char',selector_start_char,
        'selector_end_char',selector_end_char,
        'input_payload',jsonb_build_object(
            'payload_id',input_payload_id,'digest',input_payload_digest,
            'owner_component_id',payload_owner_component_id,
            'owner_kind',payload_owner_kind,'owner_id',payload_owner_id),
        'facet_content_digest',facet_content_digest,
        'input_byte_length',input_byte_length,'media_type',input_media_type,
        'width',width,'height',height
    ) ORDER BY created_at,facet_id),'[]'::jsonb),
        max(accepted_deletion_epoch),min(purpose)
    INTO selected_facets,deletion_epoch_value,selected_purpose
    FROM ranked WHERE image_bytes<=8388608;
    IF jsonb_array_length(selected_facets)=0 THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',true,'value',NULL);
    END IF;
    claim_value := jsonb_build_object(
        'protocol','forest.owned-work-claim/1',
        'work_id',requested_token,'claim_token',requested_token,'attempt',1,
        'operation_id','memory-embedding-work:'||encode(sha256(convert_to(
            requested_bot||'/'||requested_token,'UTF8')),'hex'),
        'causal_root_kind','memory.embedding.batch','causal_root_id',requested_token,
        'payload',jsonb_build_object(
            'claim_id',requested_token,'space_id',domain_space,'purpose',selected_purpose,
            'source_epoch_high_watermark',deletion_epoch_value,
            'facets',selected_facets));
    INSERT INTO __forest_module_schema__.embedding_claims(
        bot_id,claim_id,purpose,space_id,source_epoch_high_watermark,
        lease_seconds,lease_deadline,item_limit,item_count,claim_payload)
    VALUES(requested_bot,requested_token,selected_purpose,domain_space,deletion_epoch_value,
        requested_lease,clock_timestamp()+(requested_lease*interval '1 second'),
        32,jsonb_array_length(selected_facets),claim_value);
    INSERT INTO __forest_module_schema__.embedding_claim_items(
        bot_id,claim_id,item_index,facet_id)
    SELECT requested_bot,requested_token,ordinality-1,facet->>'facet_id'
    FROM jsonb_array_elements(selected_facets) WITH ORDINALITY AS item(facet,ordinality);
    RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',true,
        'value',claim_value);
EXCEPTION WHEN invalid_text_representation OR numeric_value_out_of_range THEN
    RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
        'error',jsonb_build_object('code','MEMORY_EMBEDDING_CLAIM_INVALID',
            'message','Embedding batch claim is invalid','status',422));
END;
$$;

CREATE FUNCTION memory_claim_active_embedding_batch(context jsonb, input jsonb)
RETURNS jsonb LANGUAGE plpgsql VOLATILE SET search_path = pg_catalog AS $$
BEGIN
    -- A stored domain alone is insufficient: only a successful provider-backed
    -- domain.bind owned task activates autonomous embedding dispatch.
    IF NOT EXISTS (
        SELECT 1 FROM __forest_module_schema__.memory_domain_bind_work
        WHERE bot_id=context->>'bot_id' AND state='READY'
    ) THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',true,
            'value',NULL);
    END IF;
    RETURN __forest_module_schema__.memory_claim_embedding_batch(context,input);
END;
$$;

CREATE FUNCTION memory_claim_embedding_recovery(context jsonb, input jsonb)
RETURNS jsonb LANGUAGE plpgsql VOLATILE SET search_path = pg_catalog AS $$
DECLARE
    requested_bot text := context->>'bot_id';
    requested_token text := input->>'claim_token';
    requested_lease integer := (input->>'lease_seconds')::integer;
    work_row __forest_module_schema__.embedding_recovery_work%ROWTYPE;
    attempt_row __forest_module_schema__.embedding_batch_attempts%ROWTYPE;
    task_payload jsonb;
BEGIN
    IF input->>'protocol' IS DISTINCT FROM 'forest.owned-work-claim-request/1'
       OR requested_bot IS NULL OR requested_token IS NULL
       OR octet_length(requested_token) NOT BETWEEN 1 AND 256
       OR requested_lease IS NULL OR requested_lease NOT BETWEEN 3 AND 86400 THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object('code','MEMORY_EMBEDDING_RECOVERY_CLAIM_INVALID',
                'message','Embedding recovery claim is invalid','status',422));
    END IF;
    SELECT * INTO work_row FROM __forest_module_schema__.embedding_recovery_work
    WHERE bot_id=requested_bot AND claim_token=requested_token;
    IF FOUND THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',true,
            'value',work_row.claim_payload);
    END IF;
    INSERT INTO __forest_module_schema__.embedding_recovery_work(bot_id,claim_id)
    SELECT a.bot_id,a.claim_id
    FROM __forest_module_schema__.embedding_batch_attempts a
    JOIN __forest_module_schema__.embedding_claims c
      ON c.bot_id=a.bot_id AND c.claim_id=a.claim_id
    WHERE a.bot_id=requested_bot AND a.state IN ('PREPARED','DISPATCHED')
      AND c.state='CLAIMED' AND c.lease_deadline<=clock_timestamp()
    ORDER BY c.lease_deadline,a.attempt_id LIMIT 32
    ON CONFLICT (bot_id,claim_id) DO NOTHING;
    SELECT w.* INTO work_row
    FROM __forest_module_schema__.embedding_recovery_work w
    JOIN __forest_module_schema__.embedding_batch_attempts a
      ON a.bot_id=w.bot_id AND a.claim_id=w.claim_id
    WHERE w.bot_id=requested_bot AND a.state IN ('PREPARED','DISPATCHED')
      AND w.next_attempt_at<=clock_timestamp()
      AND (w.state='PENDING'
           OR (w.state='CLAIMED' AND w.lease_deadline<=clock_timestamp()))
    ORDER BY w.next_attempt_at,w.claim_id
    LIMIT 1 FOR UPDATE OF w SKIP LOCKED;
    IF NOT FOUND THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',true,
            'value',NULL);
    END IF;
    SELECT * INTO attempt_row FROM __forest_module_schema__.embedding_batch_attempts
    WHERE bot_id=requested_bot AND claim_id=work_row.claim_id FOR UPDATE;
    IF attempt_row.state NOT IN ('PREPARED','DISPATCHED') THEN
        UPDATE __forest_module_schema__.embedding_recovery_work
        SET state='SETTLED',claim_token=NULL,claim_payload=NULL,lease_deadline=NULL,
            updated_at=clock_timestamp()
        WHERE bot_id=requested_bot AND claim_id=work_row.claim_id;
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',true,
            'value',NULL);
    END IF;
    task_payload := jsonb_build_object(
        'protocol','forest.owned-work-claim/1',
        'work_id',work_row.claim_id,'claim_token',requested_token,
        'attempt',work_row.attempt_count+1,
        'operation_id','memory-embedding-recovery:'||encode(sha256(convert_to(
            requested_bot||'/'||requested_token,'UTF8')),'hex'),
        'causal_root_kind','memory.embedding.batch',
        'causal_root_id',work_row.claim_id,
        'payload',jsonb_build_object(
            'mode',CASE attempt_row.state WHEN 'PREPARED' THEN 'ABANDON'
                ELSE 'OBSERVE_UNKNOWN' END,
            'claim_id',work_row.claim_id,
            'attempt_id',attempt_row.attempt_id,
            'effect_operation_id',attempt_row.effect_operation_id,
            'reservation_id',attempt_row.reservation_id));
    UPDATE __forest_module_schema__.embedding_recovery_work
    SET state='CLAIMED',claim_token=requested_token,claim_payload=task_payload,
        lease_deadline=clock_timestamp()+(requested_lease*interval '1 second'),
        attempt_count=attempt_count+1,updated_at=clock_timestamp()
    WHERE bot_id=requested_bot AND claim_id=work_row.claim_id;
    RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',true,
        'value',task_payload);
EXCEPTION WHEN invalid_text_representation OR numeric_value_out_of_range THEN
    RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
        'error',jsonb_build_object('code','MEMORY_EMBEDDING_RECOVERY_CLAIM_INVALID',
            'message','Embedding recovery claim is invalid','status',422));
END;
$$;

CREATE FUNCTION memory_settle_embedding_work(context jsonb, input jsonb)
RETURNS jsonb LANGUAGE plpgsql VOLATILE SET search_path = pg_catalog AS $$
DECLARE
    requested_bot text := context->>'bot_id';
    requested_token text := input->>'claim_token';
    prior_input jsonb;
    prior_result jsonb;
    result_value jsonb;
    recovery_row __forest_module_schema__.embedding_recovery_work%ROWTYPE;
    claim_row __forest_module_schema__.embedding_claims%ROWTYPE;
    attempt_row __forest_module_schema__.embedding_batch_attempts%ROWTYPE;
BEGIN
    IF input->>'protocol' IS DISTINCT FROM 'forest.owned-work-settle-request/1'
       OR requested_bot IS NULL OR requested_token IS NULL
       OR input->>'work_id' IS NULL
       OR input->>'attempt' !~ '^[1-9][0-9]*$'
       OR input#>>'{outcome,status}' IS NULL
       OR input#>>'{outcome,status}' NOT IN ('SUCCEEDED','FAILED') THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object('code','MEMORY_EMBEDDING_SETTLEMENT_INVALID',
                'message','Embedding work settlement is invalid','status',422));
    END IF;
    SELECT input_payload,result_payload INTO prior_input,prior_result
    FROM __forest_module_schema__.embedding_work_settlements
    WHERE bot_id=requested_bot AND claim_token=requested_token;
    IF FOUND THEN
        IF prior_input=input THEN RETURN prior_result; END IF;
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object('code','MEMORY_EMBEDDING_SETTLEMENT_CONFLICT',
                'message','Embedding work was settled differently','status',409));
    END IF;
    SELECT * INTO recovery_row FROM __forest_module_schema__.embedding_recovery_work
    WHERE bot_id=requested_bot AND claim_token=requested_token FOR UPDATE;
    IF FOUND THEN
        IF recovery_row.state<>'CLAIMED'
           OR input->>'work_id' IS DISTINCT FROM recovery_row.claim_id
           OR (input->>'attempt')::integer IS DISTINCT FROM recovery_row.attempt_count THEN
            RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
                'error',jsonb_build_object('code','MEMORY_EMBEDDING_RECOVERY_CLAIM_LOST',
                    'message','Embedding recovery claim is no longer current','status',409));
        END IF;
        SELECT * INTO attempt_row FROM __forest_module_schema__.embedding_batch_attempts
        WHERE bot_id=requested_bot AND claim_id=recovery_row.claim_id FOR UPDATE;
        IF attempt_row.state IN ('PREPARED','DISPATCHED') THEN
            UPDATE __forest_module_schema__.embedding_recovery_work
            SET state='PENDING',claim_token=NULL,claim_payload=NULL,lease_deadline=NULL,
                next_attempt_at=clock_timestamp()+
                    (LEAST(power(2,LEAST(attempt_count,8)),300)*interval '1 second'),
                updated_at=clock_timestamp()
            WHERE bot_id=requested_bot AND claim_id=recovery_row.claim_id;
        ELSE
            UPDATE __forest_module_schema__.embedding_recovery_work
            SET state='SETTLED',claim_token=NULL,claim_payload=NULL,lease_deadline=NULL,
                updated_at=clock_timestamp()
            WHERE bot_id=requested_bot AND claim_id=recovery_row.claim_id;
        END IF;
    ELSE
        SELECT * INTO claim_row FROM __forest_module_schema__.embedding_claims
        WHERE bot_id=requested_bot AND claim_id=requested_token FOR UPDATE;
        IF claim_row IS NULL OR input->>'work_id' IS DISTINCT FROM requested_token
           OR input->>'attempt' IS DISTINCT FROM '1' THEN
            RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
                'error',jsonb_build_object('code','MEMORY_EMBEDDING_CLAIM_LOST',
                    'message','Embedding batch claim is no longer current','status',409));
        END IF;
        SELECT * INTO attempt_row FROM __forest_module_schema__.embedding_batch_attempts
        WHERE bot_id=requested_bot AND claim_id=requested_token FOR UPDATE;
        IF attempt_row IS NULL AND claim_row.state='CLAIMED' THEN
            UPDATE __forest_module_schema__.embedding_claims
            SET state='EXPIRED' WHERE bot_id=requested_bot AND claim_id=requested_token;
            UPDATE __forest_module_schema__.embedding_facets f
            SET next_attempt_at=CASE
                    WHEN input#>>'{outcome,error,code}'='MEMORY_EMBEDDING_BUDGET_EXHAUSTED'
                    THEN (date_trunc('day',clock_timestamp() AT TIME ZONE 'UTC')
                          +interval '1 day') AT TIME ZONE 'UTC'
                    ELSE clock_timestamp()+interval '1 minute' END,
                updated_at=clock_timestamp()
            FROM __forest_module_schema__.embedding_claim_items ci
            WHERE ci.bot_id=requested_bot AND ci.claim_id=requested_token
              AND f.bot_id=ci.bot_id AND f.facet_id=ci.facet_id
              AND f.state='PENDING';
        END IF;
    END IF;
    result_value := jsonb_build_object('protocol','forest.domain-result/1','ok',true,
        'value',jsonb_build_object('protocol','forest.owned-work-settled/1',
            'work_id',input->>'work_id'));
    INSERT INTO __forest_module_schema__.embedding_work_settlements(
        bot_id,claim_token,input_payload,result_payload)
    VALUES(requested_bot,requested_token,input,result_value);
    RETURN result_value;
EXCEPTION WHEN invalid_text_representation OR numeric_value_out_of_range THEN
    RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
        'error',jsonb_build_object('code','MEMORY_EMBEDDING_SETTLEMENT_INVALID',
            'message','Embedding work settlement is invalid','status',422));
END;
$$;

CREATE FUNCTION memory_settle_embedding_batch(context jsonb, input jsonb)
RETURNS jsonb LANGUAGE sql VOLATILE SET search_path = pg_catalog AS $$
    SELECT __forest_module_schema__.memory_settle_embedding_work(context,input)
$$;

CREATE FUNCTION memory_settle_embedding_recovery(context jsonb, input jsonb)
RETURNS jsonb LANGUAGE sql VOLATILE SET search_path = pg_catalog AS $$
    SELECT __forest_module_schema__.memory_settle_embedding_work(context,input)
$$;

CREATE FUNCTION memory_read_embedding_candidates(context jsonb, input jsonb)
RETURNS jsonb LANGUAGE plpgsql STABLE SET search_path = pg_catalog AS $$
DECLARE
    requested_bot text := context->>'bot_id';
    requested_limit integer := (input->>'limit')::integer;
    domain_space text;
    candidates jsonb;
    candidate_count integer;
BEGIN
    IF requested_bot IS NULL OR requested_limit IS NULL
       OR requested_limit NOT BETWEEN 1 AND 32 THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object('code','MEMORY_EMBEDDING_CANDIDATES_INVALID',
                'message','Embedding candidate limit is invalid','status',422));
    END IF;
    SELECT space_id INTO domain_space FROM __forest_module_schema__.memory_domains
    WHERE bot_id=requested_bot;
    IF NOT FOUND THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',true,
            'value',jsonb_build_object('status','UNBOUND','space_id',NULL,
                'facets','[]'::jsonb,'has_more',false));
    END IF;
    WITH selected AS (
        SELECT f.*,u.trunk_id,u.author_subject_ref,p.width,p.height,
               p.payload_owner_component_id,p.payload_owner_kind,p.payload_owner_id,
               o.conversation_ref
        FROM __forest_module_schema__.embedding_facets f
        JOIN __forest_module_schema__.source_units u
          ON u.bot_id=f.bot_id AND u.source_unit_id=f.source_unit_id
         AND u.source_revision=f.source_revision AND u.eligibility_state='ELIGIBLE'
        JOIN __forest_module_schema__.memory_sources s
          ON s.bot_id=u.bot_id AND s.source_id=u.source_id AND s.status='ACTIVE'
        JOIN __forest_module_schema__.source_parts p
          ON p.bot_id=f.bot_id AND p.source_unit_id=f.source_unit_id
         AND p.part_id=f.part_id AND p.payload_id=f.input_payload_id
         AND p.payload_digest=f.input_payload_digest
        JOIN LATERAL (
            SELECT occurrence.conversation_ref
            FROM __forest_module_schema__.source_occurrences occurrence
            WHERE occurrence.bot_id=f.bot_id
              AND occurrence.source_unit_id=f.source_unit_id
              AND occurrence.eligibility_state='ELIGIBLE'
            ORDER BY occurrence.observed_at,occurrence.occurrence_id LIMIT 1
        ) o ON true
        WHERE f.bot_id=requested_bot AND f.purpose='DOCUMENT'
          AND f.state='PENDING' AND f.space_id=domain_space
        ORDER BY f.created_at,f.facet_id LIMIT requested_limit+1
    )
    SELECT COALESCE(jsonb_agg(jsonb_build_object(
        'facet_id',facet_id,'source_unit_id',source_unit_id,
        'source_revision',source_revision,'part_id',part_id,
        'trunk_id',trunk_id,'conversation_ref',conversation_ref,
        'author_ref',author_subject_ref,'modality',modality,
        'selector_start_char',selector_start_char,
        'selector_end_char',selector_end_char,
        'input_payload',jsonb_build_object(
            'payload_id',input_payload_id,'digest',input_payload_digest,
            'owner_component_id',payload_owner_component_id,
            'owner_kind',payload_owner_kind,'owner_id',payload_owner_id),
        'facet_content_digest',facet_content_digest,
        'input_byte_length',input_byte_length,'media_type',input_media_type,
        'width',width,'height',height
    ) ORDER BY created_at,facet_id)
        FILTER (WHERE ordinal <= requested_limit),'[]'::jsonb),count(*)
    INTO candidates,candidate_count
    FROM (
        SELECT selected.*,row_number() OVER (ORDER BY created_at,facet_id) AS ordinal
        FROM selected
    ) page;
    RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',true,
        'value',jsonb_build_object('status','READY','space_id',domain_space,
            'facets',candidates,'has_more',candidate_count>requested_limit));
EXCEPTION WHEN invalid_text_representation OR numeric_value_out_of_range THEN
    RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
        'error',jsonb_build_object('code','MEMORY_EMBEDDING_CANDIDATES_INVALID',
            'message','Embedding candidate limit is invalid','status',422));
END;
$$;

CREATE FUNCTION memory_read_exact_sources(context jsonb, input jsonb)
RETURNS jsonb
LANGUAGE plpgsql
STABLE
SET search_path = pg_catalog
AS $$
DECLARE
    requested_bot text := context->>'bot_id';
    requested_ids jsonb := input->'source_unit_ids';
    evidence_value jsonb;
    missing_value jsonb;
BEGIN
    IF jsonb_typeof(requested_ids) IS DISTINCT FROM 'array'
       OR jsonb_array_length(requested_ids) NOT BETWEEN 1 AND 8
       OR (SELECT count(*)<>count(DISTINCT value)
           FROM jsonb_array_elements_text(requested_ids)) THEN
        RETURN jsonb_build_object(
            'protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object(
                'code','MEMORY_EXACT_RECALL_INPUT_INVALID',
                'message','Exact recall requires one to eight unique source unit IDs',
                'status',422
            )
        );
    END IF;
    WITH requested AS (
        SELECT value AS source_unit_id,ordinality
        FROM jsonb_array_elements_text(requested_ids) WITH ORDINALITY
    ), found AS (
        SELECT r.ordinality,jsonb_build_object(
            'source_unit_id',u.source_unit_id,
            'source_revision',u.source_revision,
            'trunk_id',u.trunk_id,
            'source_id',u.source_id,
            'source_kind',s.source_kind,
            'platform',s.platform,
            'realm_id',s.realm_id,
            'external_group_id',s.external_group_id,
            'author_subject_ref',u.author_subject_ref,
            'occurred_at',u.occurred_at,
            'parts',COALESCE((
                SELECT jsonb_agg(jsonb_build_object(
                    'part_id',p.part_id,
                    'part_index',p.part_index,
                    'part_kind',p.part_kind,
                    'media_type',p.media_type,
                    'payload',jsonb_build_object(
                        'payload_id',p.payload_id,
                        'digest',p.payload_digest,
                        'owner_component_id',p.payload_owner_component_id,
                        'owner_kind',p.payload_owner_kind,
                        'owner_id',p.payload_owner_id
                    ),
                    'width',p.width,
                    'height',p.height
                ) ORDER BY p.part_index,p.part_id)
                FROM __forest_module_schema__.source_parts p
                WHERE p.bot_id=requested_bot AND p.source_unit_id=u.source_unit_id
            ),'[]'::jsonb)
        ) AS document
        FROM requested r
        JOIN __forest_module_schema__.source_units u
          ON u.bot_id=requested_bot AND u.source_unit_id=r.source_unit_id
             AND u.eligibility_state='ELIGIBLE'
        JOIN __forest_module_schema__.memory_sources s
          ON s.bot_id=u.bot_id AND s.source_id=u.source_id AND s.status='ACTIVE'
        WHERE (SELECT count(*) FROM __forest_module_schema__.source_parts p
               WHERE p.bot_id=requested_bot AND p.source_unit_id=u.source_unit_id)
              = u.part_count
    )
    SELECT COALESCE(jsonb_agg(document ORDER BY ordinality),'[]'::jsonb)
    INTO evidence_value FROM found;
    WITH requested AS (
        SELECT value AS source_unit_id,ordinality
        FROM jsonb_array_elements_text(requested_ids) WITH ORDINALITY
    )
    SELECT COALESCE(jsonb_agg(r.source_unit_id ORDER BY r.ordinality)
        FILTER (WHERE u.source_unit_id IS NULL),'[]'::jsonb)
    INTO missing_value
    FROM requested r
    LEFT JOIN (
        SELECT u.source_unit_id
        FROM __forest_module_schema__.source_units u
        JOIN __forest_module_schema__.memory_sources s
          ON s.bot_id=u.bot_id AND s.source_id=u.source_id AND s.status='ACTIVE'
        WHERE u.bot_id=requested_bot AND u.eligibility_state='ELIGIBLE'
          AND (SELECT count(*) FROM __forest_module_schema__.source_parts p
               WHERE p.bot_id=requested_bot AND p.source_unit_id=u.source_unit_id)
              = u.part_count
    ) u ON u.source_unit_id=r.source_unit_id;
    RETURN jsonb_build_object(
        'protocol','forest.domain-result/1','ok',true,
        'value',jsonb_build_object(
            'evidence',evidence_value,
            'missing_source_unit_ids',missing_value
        )
    );
END;
$$;

CREATE FUNCTION memory_list_trunks(context jsonb, input jsonb)
RETURNS jsonb LANGUAGE plpgsql STABLE SET search_path = pg_catalog AS $$
DECLARE result jsonb;
BEGIN
    SELECT COALESCE(jsonb_agg(jsonb_build_object(
        'trunk_id',q.trunk_id,'source_id',q.source_id,
        'platform',q.platform,'realm_id',q.realm_id,
        'external_group_id',q.external_group_id,
        'registered_sources',q.registered_sources,
        'eligible_sources',q.eligible_sources
    ) ORDER BY q.trunk_id),'[]'::jsonb) INTO result
    FROM (
        SELECT t.trunk_id,t.source_id,s.platform,s.realm_id,s.external_group_id,
            count(u.source_unit_id) AS registered_sources,
            count(u.source_unit_id) FILTER (
                WHERE u.eligibility_state='ELIGIBLE' AND s.status='ACTIVE'
            ) AS eligible_sources
        FROM __forest_module_schema__.memory_trunks t
        JOIN __forest_module_schema__.memory_sources s
            ON s.bot_id=t.bot_id AND s.source_id=t.source_id
        LEFT JOIN __forest_module_schema__.source_units u
            ON u.bot_id=t.bot_id AND u.trunk_id=t.trunk_id
        WHERE t.bot_id=context->>'bot_id'
        GROUP BY t.trunk_id,t.source_id,s.platform,s.realm_id,s.external_group_id
        ORDER BY t.trunk_id LIMIT 4096
    ) q;
    RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',true,
        'value',jsonb_build_object('items',result));
END;
$$;

CREATE FUNCTION memory_query_sources(context jsonb, input jsonb)
RETURNS jsonb LANGUAGE plpgsql STABLE SET search_path = pg_catalog AS $$
DECLARE
    page_limit integer := COALESCE((input->>'limit')::integer,50);
    snapshot_time timestamptz := COALESCE((input->>'snapshot_accepted_at')::timestamptz,clock_timestamp());
    filter_digest text := 'sha256:' || encode(sha256(convert_to((
        input - 'limit' - 'snapshot_accepted_at' - 'after_occurred_at'
              - 'after_source_unit_id' - 'cursor_filter_digest'
    )::text,'UTF8')),'hex');
    from_time timestamptz := (input->>'occurred_from')::timestamptz;
    to_time timestamptz := (input->>'occurred_to')::timestamptz;
    after_time timestamptz := (input->>'after_occurred_at')::timestamptz;
    items jsonb;
    more boolean;
    cursor_value jsonb;
BEGIN
    IF page_limit NOT BETWEEN 1 AND 100 OR
       ((after_time IS NOT NULL) <> (input ? 'after_source_unit_id')) OR
       (after_time IS NOT NULL AND (NOT input ? 'snapshot_accepted_at' OR
          input->>'cursor_filter_digest' IS DISTINCT FROM filter_digest)) THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object('code','MEMORY_QUERY_INVALID',
                'message','Source page limit or cursor is invalid','status',422));
    END IF;
    WITH page AS (
        SELECT u.source_unit_id,u.source_id,u.trunk_id,s.source_kind,
            s.platform,s.realm_id,s.external_group_id,u.author_subject_ref,
            u.unit_kind,u.modality,u.occurred_at,u.source_order_seq,u.part_count
        FROM __forest_module_schema__.source_units u
        JOIN __forest_module_schema__.memory_sources s
            ON s.bot_id=u.bot_id AND s.source_id=u.source_id
        WHERE u.bot_id=context->>'bot_id' AND u.eligibility_state='ELIGIBLE'
          AND s.status='ACTIVE'
          AND u.accepted_at<=snapshot_time AND u.eligible_at<=snapshot_time
          AND (input->>'trunk_id' IS NULL OR u.trunk_id=input->>'trunk_id')
          AND (input->>'author_subject_ref' IS NULL OR u.author_subject_ref=input->>'author_subject_ref')
          AND (input->>'modality' IS NULL OR u.modality=input->>'modality')
          AND (input->>'unit_kind' IS NULL OR u.unit_kind=input->>'unit_kind')
          AND (from_time IS NULL OR u.occurred_at>=from_time)
          AND (to_time IS NULL OR u.occurred_at<to_time)
          AND (after_time IS NULL OR (u.occurred_at,u.source_unit_id)<(after_time,input->>'after_source_unit_id'))
        ORDER BY u.occurred_at DESC,u.source_unit_id DESC LIMIT page_limit+1
    ), visible AS (
        SELECT * FROM page ORDER BY occurred_at DESC,source_unit_id DESC LIMIT page_limit
    )
    SELECT COALESCE((SELECT jsonb_agg(to_jsonb(v) ORDER BY v.occurred_at DESC,v.source_unit_id DESC)
                     FROM visible v),'[]'::jsonb),
           (SELECT count(*)>page_limit FROM page),
           (SELECT jsonb_build_object('occurred_at',v.occurred_at,
                       'source_unit_id',v.source_unit_id)
            FROM visible v ORDER BY v.occurred_at,v.source_unit_id LIMIT 1)
    INTO items,more,cursor_value;
    RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',true,
        'value',jsonb_build_object('snapshot_accepted_at',snapshot_time,
            'filter_digest',filter_digest,
            'items',items,'has_more',more,
            'next_cursor',CASE WHEN more THEN cursor_value ELSE NULL END));
END;
$$;

CREATE FUNCTION memory_query_relations(context jsonb, input jsonb)
RETURNS jsonb LANGUAGE plpgsql STABLE SET search_path = pg_catalog AS $$
DECLARE
    page_limit integer := COALESCE((input->>'limit')::integer,10);
    snapshot_time timestamptz := COALESCE((input->>'snapshot_created_at')::timestamptz,clock_timestamp());
    filter_digest text := 'sha256:' || encode(sha256(convert_to((
        input - 'limit' - 'snapshot_created_at' - 'after_relation_id'
              - 'after_contribution_id' - 'cursor_filter_digest'
    )::text,'UTF8')),'hex');
    items jsonb;
    more boolean;
    cursor_value jsonb;
BEGIN
    IF page_limit NOT BETWEEN 1 AND 20 OR
       ((input ? 'after_relation_id') <> (input ? 'after_contribution_id')) OR
       (input ? 'after_relation_id' AND (NOT input ? 'snapshot_created_at' OR
          input->>'cursor_filter_digest' IS DISTINCT FROM filter_digest)) THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object('code','MEMORY_QUERY_INVALID',
                'message','Relation page limit or cursor is invalid','status',422));
    END IF;
    WITH page AS (
        SELECT c.contribution_id,c.relation_id,c.relation_kind,
            c.left_kind,c.left_id,c.right_kind,c.right_id,c.weight_micros
        FROM __forest_module_schema__.relation_contributions c
        WHERE c.bot_id=context->>'bot_id' AND c.active AND c.created_at<=snapshot_time
          AND ((c.left_kind=input->>'endpoint_kind' AND c.left_id=input->>'endpoint_id')
            OR (c.right_kind=input->>'endpoint_kind' AND c.right_id=input->>'endpoint_id'))
          AND (input->>'endpoint_kind'<>'SOURCE_UNIT' OR EXISTS (
            SELECT 1 FROM __forest_module_schema__.source_units ue
            JOIN __forest_module_schema__.memory_sources se
              ON se.bot_id=ue.bot_id AND se.source_id=ue.source_id
            WHERE ue.bot_id=c.bot_id AND ue.source_unit_id=input->>'endpoint_id'
              AND ue.eligibility_state='ELIGIBLE' AND se.status='ACTIVE'
          ))
          AND (c.left_kind<>'SOURCE_UNIT' OR EXISTS (
            SELECT 1 FROM __forest_module_schema__.source_units ul
            JOIN __forest_module_schema__.memory_sources sl
              ON sl.bot_id=ul.bot_id AND sl.source_id=ul.source_id
            WHERE ul.bot_id=c.bot_id AND ul.source_unit_id=c.left_id
              AND ul.eligibility_state='ELIGIBLE' AND sl.status='ACTIVE'
          ))
          AND (c.right_kind<>'SOURCE_UNIT' OR EXISTS (
            SELECT 1 FROM __forest_module_schema__.source_units ur
            JOIN __forest_module_schema__.memory_sources sr
              ON sr.bot_id=ur.bot_id AND sr.source_id=ur.source_id
            WHERE ur.bot_id=c.bot_id AND ur.source_unit_id=c.right_id
              AND ur.eligibility_state='ELIGIBLE' AND sr.status='ACTIVE'
          ))
          AND (NOT input ? 'after_relation_id' OR
            (c.relation_id,c.contribution_id)>(input->>'after_relation_id',input->>'after_contribution_id'))
          AND NOT EXISTS (
            SELECT 1 FROM __forest_module_schema__.relation_evidence e0
            JOIN __forest_module_schema__.source_units u0
              ON u0.bot_id=e0.bot_id AND u0.source_unit_id=e0.source_unit_id
            JOIN __forest_module_schema__.memory_sources s0
              ON s0.bot_id=u0.bot_id AND s0.source_id=u0.source_id
            WHERE e0.bot_id=c.bot_id AND e0.contribution_id=c.contribution_id
              AND (u0.eligibility_state<>'ELIGIBLE' OR s0.status<>'ACTIVE')
          )
        ORDER BY c.relation_id,c.contribution_id LIMIT page_limit+1
    ), visible AS (
        SELECT * FROM page ORDER BY relation_id,contribution_id LIMIT page_limit
    )
    SELECT COALESCE((SELECT jsonb_agg(to_jsonb(v)||jsonb_build_object(
        'evidence_count',(
            SELECT count(*) FROM __forest_module_schema__.relation_evidence e
            WHERE e.bot_id=context->>'bot_id' AND e.contribution_id=v.contribution_id
        ),
        'evidence',COALESCE((
            SELECT jsonb_agg(to_jsonb(evidence)-'evidence_index' ORDER BY evidence.evidence_index)
            FROM (
                SELECT e.evidence_index,e.source_unit_id,e.source_revision,e.part_id,
                    u.trunk_id,s.external_group_id,u.author_subject_ref
                FROM __forest_module_schema__.relation_evidence e
                JOIN __forest_module_schema__.source_units u
                  ON u.bot_id=e.bot_id AND u.source_unit_id=e.source_unit_id
                JOIN __forest_module_schema__.memory_sources s
                  ON s.bot_id=u.bot_id AND s.source_id=u.source_id
                WHERE e.bot_id=context->>'bot_id' AND e.contribution_id=v.contribution_id
                ORDER BY e.evidence_index LIMIT 16
            ) evidence
        ),'[]'::jsonb)) ORDER BY v.relation_id,v.contribution_id)
        FROM visible v),'[]'::jsonb),
        (SELECT count(*)>page_limit FROM page),
        (SELECT jsonb_build_object('relation_id',v.relation_id,
            'contribution_id',v.contribution_id)
         FROM visible v ORDER BY v.relation_id DESC,v.contribution_id DESC LIMIT 1)
    INTO items,more,cursor_value;
    RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',true,
        'value',jsonb_build_object('snapshot_created_at',snapshot_time,
            'filter_digest',filter_digest,
            'items',items,'has_more',more,
            'next_cursor',CASE WHEN more THEN cursor_value ELSE NULL END));
END;
$$;
