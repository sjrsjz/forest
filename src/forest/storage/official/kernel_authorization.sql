CREATE TABLE policy_clock (
    bot_id text PRIMARY KEY REFERENCES forest_kernel.bot_identity(bot_id),
    policy_revision bigint NOT NULL DEFAULT 1 CHECK (policy_revision > 0)
);

CREATE TABLE grant_sets (
    bot_id text NOT NULL REFERENCES forest_kernel.bot_identity(bot_id),
    grant_set_id text NOT NULL CHECK (grant_set_id ~ '^grant:[a-f0-9]{64}$'),
    grant_set_digest text NOT NULL CHECK (grant_set_digest ~ '^sha256:[a-f0-9]{64}$'),
    grantee_kind text NOT NULL CHECK (octet_length(grantee_kind) BETWEEN 1 AND 64),
    grantee_id text NOT NULL CHECK (octet_length(grantee_id) BETWEEN 1 AND 256),
    audience_component_id text CHECK (
        audience_component_id IS NULL OR octet_length(audience_component_id) BETWEEN 1 AND 128
    ),
    audience_generation_id text CHECK (
        audience_generation_id IS NULL OR octet_length(audience_generation_id) BETWEEN 1 AND 256
    ),
    policy_revision bigint NOT NULL CHECK (policy_revision > 0),
    delegation_depth integer NOT NULL DEFAULT 0 CHECK (delegation_depth BETWEEN 0 AND 16),
    expires_at timestamptz,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (bot_id, grant_set_id),
    UNIQUE (bot_id, grant_set_digest),
    CHECK (
        (audience_component_id IS NULL) = (audience_generation_id IS NULL)
    ),
    CHECK (expires_at IS NULL OR expires_at > created_at)
);

CREATE TABLE grant_entries (
    bot_id text NOT NULL,
    grant_set_id text NOT NULL,
    entry_index integer NOT NULL CHECK (entry_index BETWEEN 0 AND 255),
    capability text NOT NULL CHECK (octet_length(capability) BETWEEN 1 AND 200),
    resource_kind text NOT NULL CHECK (octet_length(resource_kind) BETWEEN 1 AND 64),
    resource_id text NOT NULL CHECK (octet_length(resource_id) BETWEEN 1 AND 512),
    delegable boolean NOT NULL DEFAULT false,
    PRIMARY KEY (bot_id, grant_set_id, entry_index),
    UNIQUE (bot_id, grant_set_id, capability, resource_kind, resource_id),
    FOREIGN KEY (bot_id, grant_set_id)
        REFERENCES grant_sets(bot_id, grant_set_id)
);

CREATE TABLE tickets (
    bot_id text NOT NULL REFERENCES forest_kernel.bot_identity(bot_id),
    ticket_id text NOT NULL CHECK (octet_length(ticket_id) BETWEEN 1 AND 256),
    issuer_kind text NOT NULL CHECK (octet_length(issuer_kind) BETWEEN 1 AND 64),
    issuer_id text NOT NULL CHECK (octet_length(issuer_id) BETWEEN 1 AND 256),
    subject_kind text NOT NULL CHECK (octet_length(subject_kind) BETWEEN 1 AND 64),
    subject_id text NOT NULL CHECK (octet_length(subject_id) BETWEEN 1 AND 256),
    audience_component_id text CHECK (
        audience_component_id IS NULL OR octet_length(audience_component_id) BETWEEN 1 AND 128
    ),
    audience_generation_id text CHECK (
        audience_generation_id IS NULL OR octet_length(audience_generation_id) BETWEEN 1 AND 256
    ),
    action text NOT NULL CHECK (octet_length(action) BETWEEN 1 AND 200),
    resource_kind text NOT NULL CHECK (octet_length(resource_kind) BETWEEN 1 AND 64),
    resource_id text NOT NULL CHECK (octet_length(resource_id) BETWEEN 1 AND 512),
    operation_id text NOT NULL CHECK (octet_length(operation_id) BETWEEN 1 AND 256),
    input_digest text NOT NULL CHECK (input_digest ~ '^sha256:[a-f0-9]{64}$'),
    plan_digest text,
    grant_set_id text NOT NULL,
    grant_set_digest text NOT NULL CHECK (grant_set_digest ~ '^sha256:[a-f0-9]{64}$'),
    policy_revision bigint NOT NULL CHECK (policy_revision > 0),
    issued_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    expires_at timestamptz NOT NULL,
    PRIMARY KEY (bot_id, ticket_id),
    UNIQUE (bot_id, operation_id),
    FOREIGN KEY (bot_id, grant_set_id)
        REFERENCES grant_sets(bot_id, grant_set_id),
    CHECK ((audience_component_id IS NULL) = (audience_generation_id IS NULL)),
    CHECK (plan_digest IS NULL OR plan_digest ~ '^sha256:[a-f0-9]{64}$'),
    CHECK (expires_at > issued_at AND expires_at <= issued_at + interval '30 seconds')
);

CREATE TABLE ticket_consumptions (
    bot_id text NOT NULL,
    ticket_id text NOT NULL,
    operation_id text NOT NULL CHECK (octet_length(operation_id) BETWEEN 1 AND 256),
    action text NOT NULL CHECK (octet_length(action) BETWEEN 1 AND 200),
    resource_kind text NOT NULL CHECK (octet_length(resource_kind) BETWEEN 1 AND 64),
    resource_id text NOT NULL CHECK (octet_length(resource_id) BETWEEN 1 AND 512),
    audience_component_id text CHECK (
        audience_component_id IS NULL OR octet_length(audience_component_id) BETWEEN 1 AND 128
    ),
    audience_generation_id text CHECK (
        audience_generation_id IS NULL OR octet_length(audience_generation_id) BETWEEN 1 AND 256
    ),
    input_digest text NOT NULL CHECK (input_digest ~ '^sha256:[a-f0-9]{64}$'),
    plan_digest text,
    policy_revision bigint NOT NULL CHECK (policy_revision > 0),
    consumed_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (bot_id, ticket_id),
    UNIQUE (bot_id, operation_id),
    FOREIGN KEY (bot_id, ticket_id) REFERENCES tickets(bot_id, ticket_id),
    CHECK ((audience_component_id IS NULL) = (audience_generation_id IS NULL)),
    CHECK (plan_digest IS NULL OR plan_digest ~ '^sha256:[a-f0-9]{64}$')
);

CREATE TABLE revocations (
    bot_id text NOT NULL REFERENCES forest_kernel.bot_identity(bot_id),
    operation_id text NOT NULL CHECK (octet_length(operation_id) BETWEEN 1 AND 256),
    input_digest text NOT NULL CHECK (input_digest ~ '^sha256:[a-f0-9]{64}$'),
    previous_revision bigint NOT NULL CHECK (previous_revision > 0),
    policy_revision bigint NOT NULL CHECK (policy_revision = previous_revision + 1),
    selector_kind text NOT NULL CHECK (octet_length(selector_kind) BETWEEN 1 AND 64),
    selector_id text NOT NULL CHECK (octet_length(selector_id) BETWEEN 1 AND 512),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (bot_id, operation_id),
    UNIQUE (bot_id, policy_revision)
);

CREATE TABLE approvals (
    bot_id text NOT NULL REFERENCES forest_kernel.bot_identity(bot_id),
    approval_id text NOT NULL CHECK (octet_length(approval_id) BETWEEN 1 AND 256),
    request_kind text NOT NULL CHECK (octet_length(request_kind) BETWEEN 1 AND 64),
    request_id text NOT NULL CHECK (octet_length(request_id) BETWEEN 1 AND 256),
    action text NOT NULL CHECK (octet_length(action) BETWEEN 1 AND 200),
    input_digest text NOT NULL CHECK (input_digest ~ '^sha256:[a-f0-9]{64}$'),
    plan_digest text,
    requester_kind text NOT NULL CHECK (octet_length(requester_kind) BETWEEN 1 AND 64),
    requester_id text NOT NULL CHECK (octet_length(requester_id) BETWEEN 1 AND 256),
    approver_scope_kind text NOT NULL
        CHECK (octet_length(approver_scope_kind) BETWEEN 1 AND 64),
    approver_scope_id text NOT NULL
        CHECK (octet_length(approver_scope_id) BETWEEN 1 AND 512),
    policy_revision bigint NOT NULL CHECK (policy_revision > 0),
    expires_at timestamptz NOT NULL,
    state text NOT NULL DEFAULT 'PENDING'
        CHECK (state IN ('PENDING', 'APPROVED', 'REJECTED', 'CONSUMED')),
    decided_by_kind text,
    decided_by_id text,
    decision_operation_id text,
    decided_at timestamptz,
    consume_operation_id text,
    consumed_at timestamptz,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (bot_id, approval_id),
    UNIQUE (bot_id, request_kind, request_id),
    CHECK (decided_by_kind IS NULL OR octet_length(decided_by_kind) BETWEEN 1 AND 64),
    CHECK (decided_by_id IS NULL OR octet_length(decided_by_id) BETWEEN 1 AND 256),
    CHECK (decision_operation_id IS NULL OR octet_length(decision_operation_id) BETWEEN 1 AND 256),
    CHECK (consume_operation_id IS NULL OR octet_length(consume_operation_id) BETWEEN 1 AND 256),
    CHECK (plan_digest IS NULL OR plan_digest ~ '^sha256:[a-f0-9]{64}$'),
    CHECK (expires_at > created_at),
    CHECK (
        (state = 'PENDING' AND decided_by_kind IS NULL AND decided_by_id IS NULL
            AND decision_operation_id IS NULL AND decided_at IS NULL
            AND consume_operation_id IS NULL AND consumed_at IS NULL)
        OR (state IN ('APPROVED', 'REJECTED') AND decided_by_kind IS NOT NULL
            AND decided_by_id IS NOT NULL AND decision_operation_id IS NOT NULL
            AND decided_at IS NOT NULL AND consume_operation_id IS NULL
            AND consumed_at IS NULL)
        OR (state = 'CONSUMED' AND decided_by_kind IS NOT NULL
            AND decided_by_id IS NOT NULL AND decision_operation_id IS NOT NULL
            AND decided_at IS NOT NULL AND consume_operation_id IS NOT NULL
            AND consumed_at IS NOT NULL)
    )
);

CREATE INDEX tickets_expiry_idx ON tickets(bot_id, expires_at, ticket_id);
CREATE INDEX approvals_pending_idx ON approvals(bot_id, expires_at, approval_id)
    WHERE state = 'PENDING';
CREATE INDEX approvals_requester_idx ON approvals(
    bot_id, requester_kind, requester_id, created_at DESC, approval_id
);
CREATE UNIQUE INDEX approvals_decision_operation_idx ON approvals(bot_id, decision_operation_id)
    WHERE decision_operation_id IS NOT NULL;
CREATE UNIQUE INDEX approvals_consume_operation_idx ON approvals(bot_id, consume_operation_id)
    WHERE consume_operation_id IS NOT NULL;

CREATE FUNCTION reject_immutable_authorization_fact() RETURNS trigger LANGUAGE plpgsql
SET search_path FROM CURRENT AS $$
BEGIN
    RAISE EXCEPTION 'Forest authorization fact is immutable' USING ERRCODE = '55000';
END;
$$;

CREATE TRIGGER grant_sets_immutable BEFORE UPDATE OR DELETE ON grant_sets
    FOR EACH ROW EXECUTE FUNCTION reject_immutable_authorization_fact();
CREATE TRIGGER grant_entries_immutable BEFORE UPDATE OR DELETE ON grant_entries
    FOR EACH ROW EXECUTE FUNCTION reject_immutable_authorization_fact();
CREATE TRIGGER tickets_immutable BEFORE UPDATE OR DELETE ON tickets
    FOR EACH ROW EXECUTE FUNCTION reject_immutable_authorization_fact();
CREATE TRIGGER ticket_consumptions_immutable BEFORE UPDATE OR DELETE ON ticket_consumptions
    FOR EACH ROW EXECUTE FUNCTION reject_immutable_authorization_fact();
CREATE TRIGGER revocations_immutable BEFORE UPDATE OR DELETE ON revocations
    FOR EACH ROW EXECUTE FUNCTION reject_immutable_authorization_fact();

CREATE FUNCTION policy_clock_guard() RETURNS trigger LANGUAGE plpgsql
SET search_path FROM CURRENT AS $$
BEGIN
    IF TG_OP = 'DELETE' OR NEW.bot_id <> OLD.bot_id
       OR NEW.policy_revision <> OLD.policy_revision + 1
       OR NOT EXISTS (
           SELECT 1 FROM revocations r
           WHERE r.bot_id = NEW.bot_id AND r.policy_revision = NEW.policy_revision
       ) THEN
        RAISE EXCEPTION 'Authorization policy revision lacks a revocation fact'
            USING ERRCODE = '55000';
    END IF;
    RETURN NEW;
END;
$$;
CREATE TRIGGER policy_clock_monotonic BEFORE UPDATE OR DELETE ON policy_clock
    FOR EACH ROW EXECUTE FUNCTION policy_clock_guard();

CREATE FUNCTION grant_entry_guard() RETURNS trigger LANGUAGE plpgsql
SET search_path FROM CURRENT AS $$
DECLARE current_revision bigint;
BEGIN
    SELECT policy_revision INTO current_revision FROM policy_clock
        WHERE bot_id = NEW.bot_id FOR SHARE;
    IF NOT EXISTS (
        SELECT 1 FROM grant_sets g
        WHERE g.bot_id = NEW.bot_id AND g.grant_set_id = NEW.grant_set_id
          AND g.policy_revision = current_revision
    ) THEN
        RAISE EXCEPTION 'Grant entry has no current grant set' USING ERRCODE = '55000';
    END IF;
    RETURN NEW;
END;
$$;
CREATE TRIGGER grant_entry_admission BEFORE INSERT ON grant_entries
    FOR EACH ROW EXECUTE FUNCTION grant_entry_guard();

CREATE FUNCTION approval_guard() RETURNS trigger LANGUAGE plpgsql
SET search_path FROM CURRENT AS $$
BEGIN
    IF TG_OP = 'DELETE' OR (
        to_jsonb(NEW) - ARRAY[
            'state','decided_by_kind','decided_by_id','decision_operation_id',
            'decided_at','consume_operation_id','consumed_at'
        ]
    ) IS DISTINCT FROM (
        to_jsonb(OLD) - ARRAY[
            'state','decided_by_kind','decided_by_id','decision_operation_id',
            'decided_at','consume_operation_id','consumed_at'
        ]
    ) THEN
        RAISE EXCEPTION 'Approval identity is immutable' USING ERRCODE = '55000';
    END IF;
    IF OLD.state = 'PENDING' AND NEW.state IN ('APPROVED', 'REJECTED')
       AND NEW.decided_at IS NOT NULL THEN
        RETURN NEW;
    END IF;
    IF OLD.state = 'APPROVED' AND NEW.state = 'CONSUMED'
       AND NEW.decided_by_kind = OLD.decided_by_kind
       AND NEW.decided_by_id = OLD.decided_by_id
       AND NEW.decision_operation_id = OLD.decision_operation_id
       AND NEW.decided_at = OLD.decided_at
       AND NEW.consume_operation_id IS NOT NULL AND NEW.consumed_at IS NOT NULL THEN
        RETURN NEW;
    END IF;
    RAISE EXCEPTION 'Approval decision is not a legal one-way transition'
        USING ERRCODE = '55000';
END;
$$;
CREATE TRIGGER approvals_one_way BEFORE UPDATE OR DELETE ON approvals
    FOR EACH ROW EXECUTE FUNCTION approval_guard();

INSERT INTO policy_clock(bot_id)
SELECT bot_id FROM forest_kernel.bot_identity;
