-- A Bot storage plan remains the immutable provisioning fact for generation 1.
-- Later restore activations append generations and move one monotonic directory
-- head; credentials are never copied into public projections or audit details.
CREATE TABLE bot_storage_generations (
    bot_id text NOT NULL REFERENCES bot_catalog(bot_id),
    storage_generation bigint NOT NULL CHECK(storage_generation>0),
    source_kind text NOT NULL CHECK(source_kind IN ('PROVISIONED','RESTORE')),
    source_restore_id text,
    source_ordinal integer,
    plan_digest text NOT NULL,
    cluster_id text,
    database_name text NOT NULL,
    role_name text NOT NULL,
    secret_ciphertext text NOT NULL,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY(bot_id,storage_generation),
    UNIQUE(source_restore_id,source_ordinal),
    CHECK(
        source_kind='PROVISIONED' AND source_restore_id IS NULL AND source_ordinal IS NULL
        OR source_kind='RESTORE' AND source_restore_id IS NOT NULL
           AND source_ordinal BETWEEN 0 AND 1023
    )
);
INSERT INTO bot_storage_generations(
    bot_id,storage_generation,source_kind,plan_digest,cluster_id,database_name,
    role_name,secret_ciphertext,created_at
)
SELECT bot_id,1,'PROVISIONED',plan_digest,cluster_id,database_name,role_name,
       secret_ciphertext,created_at
FROM bot_storage_plans;
CREATE TRIGGER bot_storage_generation_immutable
    BEFORE UPDATE OR DELETE ON bot_storage_generations
    FOR EACH ROW EXECUTE FUNCTION forest_reject_fact_mutation();

CREATE TABLE bot_storage_heads (
    bot_id text PRIMARY KEY REFERENCES bot_catalog(bot_id),
    storage_generation bigint NOT NULL CHECK(storage_generation>0),
    revision bigint NOT NULL DEFAULT 1 CHECK(revision>0),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    FOREIGN KEY(bot_id,storage_generation)
        REFERENCES bot_storage_generations(bot_id,storage_generation)
);
INSERT INTO bot_storage_heads(bot_id,storage_generation)
SELECT bot_id,1 FROM bot_storage_plans;

CREATE FUNCTION forest_storage_head_guard() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF TG_OP='DELETE'
       OR NEW.bot_id IS DISTINCT FROM OLD.bot_id
       OR NEW.storage_generation<>OLD.storage_generation+1
       OR NEW.revision<>OLD.revision+1
       OR NEW.updated_at<OLD.updated_at THEN
        RAISE EXCEPTION 'Bot storage head advances by exactly one generation'
            USING ERRCODE='55000';
    END IF;
    RETURN NEW;
END;
$$;
CREATE TRIGGER bot_storage_head_guard
    BEFORE UPDATE OR DELETE ON bot_storage_heads
    FOR EACH ROW EXECUTE FUNCTION forest_storage_head_guard();

-- Preview facts bind the exact isolated target, current directory generation,
-- memory deletion barrier, and critical external-effect ledger digest.
CREATE TABLE restore_activation_plans (
    plan_id text PRIMARY KEY,
    subject_id text NOT NULL REFERENCES web_accounts(id),
    restore_id text NOT NULL REFERENCES restore_stage_jobs(restore_id),
    target_ordinal integer NOT NULL CHECK(target_ordinal BETWEEN 0 AND 1023),
    bot_id text NOT NULL REFERENCES bot_catalog(bot_id),
    expected_storage_generation bigint NOT NULL CHECK(expected_storage_generation>0),
    current_database_name text NOT NULL,
    target_database_name text NOT NULL,
    deletion_epoch bigint NOT NULL CHECK(deletion_epoch>=0),
    safety_digest text NOT NULL,
    quarantine_digest text NOT NULL,
    plan_digest text NOT NULL,
    expires_at timestamptz NOT NULL,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    UNIQUE(restore_id,target_ordinal,plan_id),
    CHECK(expires_at>created_at)
);
CREATE TRIGGER restore_activation_plan_immutable
    BEFORE UPDATE OR DELETE ON restore_activation_plans
    FOR EACH ROW EXECUTE FUNCTION forest_reject_fact_mutation();

CREATE TABLE restore_activation_jobs (
    activation_id text PRIMARY KEY,
    plan_id text NOT NULL UNIQUE REFERENCES restore_activation_plans(plan_id),
    subject_id text NOT NULL REFERENCES web_accounts(id),
    restore_id text NOT NULL,
    target_ordinal integer NOT NULL,
    bot_id text NOT NULL REFERENCES bot_catalog(bot_id),
    expected_storage_generation bigint NOT NULL CHECK(expected_storage_generation>0),
    target_storage_generation bigint NOT NULL CHECK(
        target_storage_generation=expected_storage_generation+1
    ),
    current_database_name text NOT NULL,
    target_database_name text NOT NULL,
    role_name text NOT NULL,
    cluster_id text NOT NULL,
    source_secret_ciphertext text NOT NULL,
    target_secret_ciphertext text NOT NULL,
    target_plan_digest text NOT NULL,
    deletion_epoch bigint NOT NULL CHECK(deletion_epoch>=0),
    safety_digest text NOT NULL,
    quarantine_digest text NOT NULL,
    input_digest text NOT NULL,
    state text NOT NULL DEFAULT 'QUEUED' CHECK(
        state IN ('QUEUED','FENCING','SWITCHING','ACTIVATING','COMPLETE','BLOCKED')
    ),
    owner_id text,
    owner_epoch bigint NOT NULL DEFAULT 0 CHECK(owner_epoch>=0),
    lease_expires_at timestamptz,
    attempt_count integer NOT NULL DEFAULT 0 CHECK(attempt_count>=0),
    error_code text,
    fenced_at timestamptz,
    switched_at timestamptz,
    activated_at timestamptz,
    completed_at timestamptz,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    FOREIGN KEY(restore_id,target_ordinal)
        REFERENCES restore_stage_targets(restore_id,ordinal),
    UNIQUE(restore_id,target_ordinal),
    CHECK((state IN ('FENCING','SWITCHING','ACTIVATING'))=(owner_id IS NOT NULL)),
    CHECK((owner_id IS NULL)=(lease_expires_at IS NULL)),
    CHECK((state='BLOCKED')=(error_code IS NOT NULL)),
    CHECK((state='COMPLETE')=(completed_at IS NOT NULL))
);
CREATE UNIQUE INDEX restore_activation_one_open_bot
    ON restore_activation_jobs(bot_id) WHERE state<>'COMPLETE';
CREATE INDEX restore_activation_resume
    ON restore_activation_jobs(state,lease_expires_at,created_at);

CREATE TABLE restore_activation_requests (
    request_id text PRIMARY KEY,
    subject_id text NOT NULL REFERENCES web_accounts(id),
    idempotency_key text NOT NULL,
    input_digest text NOT NULL,
    result jsonb NOT NULL,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    UNIQUE(subject_id,idempotency_key)
);
CREATE TRIGGER restore_activation_request_immutable
    BEFORE UPDATE OR DELETE ON restore_activation_requests
    FOR EACH ROW EXECUTE FUNCTION forest_reject_fact_mutation();

CREATE TABLE restore_activation_events (
    event_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    activation_id text NOT NULL REFERENCES restore_activation_jobs(activation_id),
    owner_epoch bigint NOT NULL,
    step text NOT NULL,
    details jsonb NOT NULL DEFAULT '{}',
    created_at timestamptz NOT NULL DEFAULT clock_timestamp()
);
CREATE TRIGGER restore_activation_event_immutable
    BEFORE UPDATE OR DELETE ON restore_activation_events
    FOR EACH ROW EXECUTE FUNCTION forest_reject_fact_mutation();

CREATE FUNCTION forest_restore_activation_job_guard() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF TG_OP='DELETE'
       OR ROW(NEW.activation_id,NEW.plan_id,NEW.subject_id,NEW.restore_id,
              NEW.target_ordinal,NEW.bot_id,NEW.expected_storage_generation,
              NEW.target_storage_generation,NEW.current_database_name,
              NEW.target_database_name,NEW.role_name,NEW.cluster_id,
              NEW.source_secret_ciphertext,NEW.target_secret_ciphertext,
              NEW.target_plan_digest,NEW.deletion_epoch,NEW.safety_digest,
              NEW.quarantine_digest,NEW.input_digest,NEW.created_at)
          IS DISTINCT FROM
          ROW(OLD.activation_id,OLD.plan_id,OLD.subject_id,OLD.restore_id,
              OLD.target_ordinal,OLD.bot_id,OLD.expected_storage_generation,
              OLD.target_storage_generation,OLD.current_database_name,
              OLD.target_database_name,OLD.role_name,OLD.cluster_id,
              OLD.source_secret_ciphertext,OLD.target_secret_ciphertext,
              OLD.target_plan_digest,OLD.deletion_epoch,OLD.safety_digest,
              OLD.quarantine_digest,OLD.input_digest,OLD.created_at)
       OR OLD.state='COMPLETE' OR NEW.owner_epoch<OLD.owner_epoch
       OR NEW.attempt_count<OLD.attempt_count THEN
        RAISE EXCEPTION 'Restore activation identity and progress are immutable'
            USING ERRCODE='55000';
    END IF;
    IF NOT (
        OLD.state='QUEUED' AND NEW.state='FENCING'
        OR OLD.state='FENCING' AND NEW.state IN ('FENCING','SWITCHING','BLOCKED')
        OR OLD.state='SWITCHING' AND NEW.state IN ('SWITCHING','ACTIVATING','BLOCKED')
        OR OLD.state='ACTIVATING' AND NEW.state IN ('ACTIVATING','COMPLETE','BLOCKED')
        OR OLD.state='BLOCKED' AND NEW.state='QUEUED'
    ) THEN
        RAISE EXCEPTION 'Invalid restore activation transition' USING ERRCODE='55000';
    END IF;
    IF NEW.owner_epoch=OLD.owner_epoch+1 THEN
        IF NEW.state NOT IN ('FENCING','SWITCHING','ACTIVATING')
           OR NEW.attempt_count<>OLD.attempt_count+1 THEN
            RAISE EXCEPTION 'Restore activation claim epoch is invalid' USING ERRCODE='55000';
        END IF;
    ELSIF NEW.owner_epoch=OLD.owner_epoch THEN
        IF NEW.attempt_count<>OLD.attempt_count THEN
            RAISE EXCEPTION 'Restore activation claim epoch is invalid' USING ERRCODE='55000';
        END IF;
    ELSE
        RAISE EXCEPTION 'Restore activation claim epoch is invalid' USING ERRCODE='55000';
    END IF;
    RETURN NEW;
END;
$$;
CREATE TRIGGER restore_activation_job_guard
    BEFORE UPDATE OR DELETE ON restore_activation_jobs
    FOR EACH ROW EXECUTE FUNCTION forest_restore_activation_job_guard();
