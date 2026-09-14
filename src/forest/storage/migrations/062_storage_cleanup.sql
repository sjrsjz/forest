-- Crash-resumable cleanup for encrypted backup artifacts and isolated restore databases.
ALTER TABLE restore_stage_jobs ADD COLUMN expires_at timestamptz;
UPDATE restore_stage_jobs SET expires_at=created_at+interval '7 days' WHERE expires_at IS NULL;
ALTER TABLE restore_stage_jobs ALTER COLUMN expires_at SET NOT NULL;
ALTER TABLE restore_stage_jobs ALTER COLUMN expires_at
    SET DEFAULT (clock_timestamp()+interval '7 days');

CREATE TABLE storage_cleanup_plans (
    plan_id text PRIMARY KEY,
    subject_id text NOT NULL REFERENCES web_accounts(id),
    resource_kind text NOT NULL CHECK(resource_kind IN ('BACKUP_ARTIFACT','RESTORE_STAGE')),
    resource_id text NOT NULL,
    resource_digest text NOT NULL,
    plan_digest text NOT NULL,
    expires_at timestamptz NOT NULL,
    applied_cleanup_id text,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    CHECK(expires_at>created_at)
);
CREATE INDEX storage_cleanup_plans_subject ON storage_cleanup_plans(subject_id,created_at DESC);

CREATE TABLE storage_cleanup_jobs (
    cleanup_id text PRIMARY KEY,
    resource_kind text NOT NULL CHECK(resource_kind IN ('BACKUP_ARTIFACT','RESTORE_STAGE')),
    resource_id text NOT NULL,
    subject_id text NOT NULL REFERENCES web_accounts(id),
    reason text NOT NULL CHECK(reason IN ('EXPLICIT','RETENTION')),
    input_digest text NOT NULL,
    state text NOT NULL DEFAULT 'QUEUED'
        CHECK(state IN ('QUEUED','CLEANING','RETRY','COMPLETE','BLOCKED')),
    owner_id text,
    owner_epoch bigint NOT NULL DEFAULT 0 CHECK(owner_epoch>=0),
    lease_expires_at timestamptz,
    attempt_count integer NOT NULL DEFAULT 0 CHECK(attempt_count>=0),
    next_attempt_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    error_code text,
    completed_at timestamptz,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    UNIQUE(resource_kind,resource_id),
    CHECK((state='CLEANING')=(owner_id IS NOT NULL)),
    CHECK((owner_id IS NULL)=(lease_expires_at IS NULL)),
    CHECK((state='COMPLETE')=(completed_at IS NOT NULL)),
    CHECK(state<>'BLOCKED' OR error_code IS NOT NULL)
);
ALTER TABLE storage_cleanup_plans ADD CONSTRAINT storage_cleanup_plan_applied_fk
    FOREIGN KEY(applied_cleanup_id) REFERENCES storage_cleanup_jobs(cleanup_id);
CREATE INDEX storage_cleanup_jobs_resume
    ON storage_cleanup_jobs(state,next_attempt_at,lease_expires_at,created_at);

CREATE TABLE storage_cleanup_requests (
    request_id text PRIMARY KEY,
    subject_id text NOT NULL REFERENCES web_accounts(id),
    idempotency_key text NOT NULL,
    input_digest text NOT NULL,
    result jsonb NOT NULL,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    UNIQUE(subject_id,idempotency_key)
);
CREATE TRIGGER storage_cleanup_requests_immutable BEFORE UPDATE OR DELETE ON storage_cleanup_requests
    FOR EACH ROW EXECUTE FUNCTION forest_reject_fact_mutation();

CREATE TABLE storage_cleanup_events (
    event_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    cleanup_id text NOT NULL REFERENCES storage_cleanup_jobs(cleanup_id),
    owner_epoch bigint NOT NULL,
    step text NOT NULL,
    details jsonb NOT NULL DEFAULT '{}',
    created_at timestamptz NOT NULL DEFAULT clock_timestamp()
);
CREATE TRIGGER storage_cleanup_events_immutable BEFORE UPDATE OR DELETE ON storage_cleanup_events
    FOR EACH ROW EXECUTE FUNCTION forest_reject_fact_mutation();

CREATE TABLE backup_orphan_cleanup_receipts (
    receipt_id text PRIMARY KEY,
    backup_id text NOT NULL REFERENCES backup_jobs(backup_id),
    owner_epoch bigint NOT NULL CHECK(owner_epoch>0),
    path_kind text NOT NULL CHECK(path_kind IN ('CANDIDATE','JOB_DIRECTORY')),
    relative_path_digest text NOT NULL UNIQUE,
    byte_size bigint NOT NULL CHECK(byte_size>=0),
    cleaned_at timestamptz NOT NULL DEFAULT clock_timestamp()
);
CREATE TRIGGER backup_orphan_cleanup_receipts_immutable
    BEFORE UPDATE OR DELETE ON backup_orphan_cleanup_receipts
    FOR EACH ROW EXECUTE FUNCTION forest_reject_fact_mutation();

CREATE FUNCTION forest_cleanup_plan_guard() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF TG_OP='DELETE'
       OR ROW(NEW.plan_id,NEW.subject_id,NEW.resource_kind,NEW.resource_id,
              NEW.resource_digest,NEW.plan_digest,NEW.expires_at,NEW.created_at)
          IS DISTINCT FROM
          ROW(OLD.plan_id,OLD.subject_id,OLD.resource_kind,OLD.resource_id,
              OLD.resource_digest,OLD.plan_digest,OLD.expires_at,OLD.created_at)
       OR OLD.applied_cleanup_id IS NOT NULL
       OR NEW.applied_cleanup_id IS NULL THEN
        RAISE EXCEPTION 'Storage cleanup plan is immutable' USING ERRCODE='55000';
    END IF;
    IF NOT EXISTS(
        SELECT 1 FROM storage_cleanup_jobs c
        WHERE c.cleanup_id=NEW.applied_cleanup_id
          AND c.resource_kind=NEW.resource_kind AND c.resource_id=NEW.resource_id
          AND c.subject_id=NEW.subject_id
    ) THEN
        RAISE EXCEPTION 'Storage cleanup plan result is unrelated' USING ERRCODE='55000';
    END IF;
    RETURN NEW;
END;
$$;
CREATE TRIGGER storage_cleanup_plan_guard BEFORE UPDATE OR DELETE ON storage_cleanup_plans
    FOR EACH ROW EXECUTE FUNCTION forest_cleanup_plan_guard();

CREATE FUNCTION forest_cleanup_job_guard() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF TG_OP='DELETE'
       OR ROW(NEW.cleanup_id,NEW.resource_kind,NEW.resource_id,NEW.subject_id,
              NEW.reason,NEW.input_digest,NEW.created_at)
          IS DISTINCT FROM
          ROW(OLD.cleanup_id,OLD.resource_kind,OLD.resource_id,OLD.subject_id,
              OLD.reason,OLD.input_digest,OLD.created_at)
       OR OLD.state='COMPLETE' THEN
        RAISE EXCEPTION 'Storage cleanup identity or terminal state is immutable' USING ERRCODE='55000';
    END IF;
    IF NEW.owner_epoch<OLD.owner_epoch OR NEW.attempt_count<OLD.attempt_count
       OR OLD.completed_at IS NOT NULL AND NEW.completed_at IS DISTINCT FROM OLD.completed_at THEN
        RAISE EXCEPTION 'Storage cleanup progress is monotonic' USING ERRCODE='55000';
    END IF;
    IF NOT (
        OLD.state IN ('QUEUED','RETRY') AND NEW.state='CLEANING'
        OR OLD.state='CLEANING' AND NEW.state IN ('CLEANING','RETRY','COMPLETE','BLOCKED')
        OR OLD.state='BLOCKED' AND NEW.state='QUEUED'
    ) THEN
        RAISE EXCEPTION 'Invalid storage cleanup transition' USING ERRCODE='55000';
    END IF;
    IF NEW.owner_epoch=OLD.owner_epoch+1 THEN
        IF NEW.state<>'CLEANING' OR NEW.attempt_count<>OLD.attempt_count+1 THEN
            RAISE EXCEPTION 'Storage cleanup claim epoch is invalid' USING ERRCODE='55000';
        END IF;
    ELSIF NEW.owner_epoch=OLD.owner_epoch THEN
        IF NEW.attempt_count<>OLD.attempt_count THEN
            RAISE EXCEPTION 'Storage cleanup claim epoch is invalid' USING ERRCODE='55000';
        END IF;
    ELSE
        RAISE EXCEPTION 'Storage cleanup claim epoch is invalid' USING ERRCODE='55000';
    END IF;
    RETURN NEW;
END;
$$;
CREATE TRIGGER storage_cleanup_job_guard BEFORE UPDATE OR DELETE ON storage_cleanup_jobs
    FOR EACH ROW EXECUTE FUNCTION forest_cleanup_job_guard();

-- EXPIRED is the logical visibility barrier. It may only be entered with a durable
-- cleanup job already present in the same transaction.
CREATE OR REPLACE FUNCTION forest_backup_job_transition() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF TG_OP='DELETE' OR OLD.state IN ('FAILED','EXPIRED')
       OR OLD.state IN ('READY','INCONSISTENT') AND NEW.state<>'EXPIRED' THEN
        RAISE EXCEPTION 'Terminal backup job is immutable' USING ERRCODE='55000';
    END IF;
    IF NOT (
        OLD.state='QUEUED' AND NEW.state IN ('CAPTURING','FAILED')
        OR OLD.state='CAPTURING' AND NEW.state IN ('CAPTURING','SEALING','FAILED')
        OR OLD.state='SEALING' AND NEW.state IN ('CAPTURING','READY','INCONSISTENT','FAILED')
        OR OLD.state IN ('READY','INCONSISTENT') AND NEW.state='EXPIRED'
    ) THEN
        RAISE EXCEPTION 'Invalid backup job state transition' USING ERRCODE='55000';
    END IF;
    IF NEW.state='EXPIRED' AND NOT EXISTS(
        SELECT 1 FROM storage_cleanup_jobs c
        WHERE c.resource_kind='BACKUP_ARTIFACT' AND c.resource_id=NEW.backup_id
    ) THEN
        RAISE EXCEPTION 'Backup expiration requires cleanup job' USING ERRCODE='55000';
    END IF;
    RETURN NEW;
END;
$$;

CREATE OR REPLACE FUNCTION forest_backup_artifact_guard() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF TG_OP='DELETE'
       OR ROW(NEW.backup_id,NEW.relative_path,NEW.ciphertext_digest,NEW.byte_size,
              NEW.manifest_digest,NEW.ready_at,NEW.expires_at)
          IS DISTINCT FROM
          ROW(OLD.backup_id,OLD.relative_path,OLD.ciphertext_digest,OLD.byte_size,
              OLD.manifest_digest,OLD.ready_at,OLD.expires_at)
       OR OLD.deleted_at IS NOT NULL AND NEW.deleted_at IS DISTINCT FROM OLD.deleted_at THEN
        RAISE EXCEPTION 'Backup artifact identity is immutable' USING ERRCODE='55000';
    END IF;
    IF OLD.deleted_at IS NULL AND NEW.deleted_at IS NOT NULL AND NOT EXISTS(
        SELECT 1 FROM storage_cleanup_jobs c
        WHERE c.resource_kind='BACKUP_ARTIFACT' AND c.resource_id=NEW.backup_id
    ) THEN
        RAISE EXCEPTION 'Backup tombstone requires cleanup job' USING ERRCODE='55000';
    END IF;
    RETURN NEW;
END;
$$;

CREATE OR REPLACE FUNCTION forest_restore_job_identity() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF TG_OP='DELETE'
       OR ROW(NEW.restore_id,NEW.backup_id,NEW.subject_id,NEW.idempotency_key,
              NEW.input_digest,NEW.artifact_digest,NEW.archive_key_ciphertext,
              NEW.database_total,NEW.expires_at,NEW.created_at)
          IS DISTINCT FROM
          ROW(OLD.restore_id,OLD.backup_id,OLD.subject_id,OLD.idempotency_key,
              OLD.input_digest,OLD.artifact_digest,OLD.archive_key_ciphertext,
              OLD.database_total,OLD.expires_at,OLD.created_at) THEN
        RAISE EXCEPTION 'Restore job identity is immutable' USING ERRCODE='55000';
    END IF;
    IF OLD.state='EXPIRED' THEN
        RAISE EXCEPTION 'Terminal restore job is immutable' USING ERRCODE='55000';
    END IF;
    IF NOT (
        OLD.state='QUEUED' AND NEW.state IN ('VERIFYING','FAILED')
        OR OLD.state='VERIFYING' AND NEW.state IN ('VERIFYING','RESTORING','FAILED')
        OR OLD.state='RESTORING' AND NEW.state IN ('VERIFYING','RESTORING','VALIDATING','FAILED')
        OR OLD.state='VALIDATING' AND NEW.state IN ('VERIFYING','VALIDATING','STAGED','FAILED')
        OR OLD.state IN ('STAGED','FAILED') AND NEW.state='EXPIRED'
    ) THEN
        RAISE EXCEPTION 'Invalid restore job state transition' USING ERRCODE='55000';
    END IF;
    IF NEW.state='EXPIRED' AND NOT EXISTS(
        SELECT 1 FROM storage_cleanup_jobs c
        WHERE c.resource_kind='RESTORE_STAGE' AND c.resource_id=NEW.restore_id
    ) THEN
        RAISE EXCEPTION 'Restore expiration requires cleanup job' USING ERRCODE='55000';
    END IF;
    RETURN NEW;
END;
$$;
