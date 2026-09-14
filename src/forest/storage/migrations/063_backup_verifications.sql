-- Read-only, expiring proof that one exact local backup artifact passed full validation.
CREATE TABLE backup_verification_jobs (
    verification_id text PRIMARY KEY,
    backup_id text NOT NULL REFERENCES backup_artifacts(backup_id),
    subject_id text NOT NULL REFERENCES web_accounts(id),
    idempotency_key text NOT NULL,
    input_digest text NOT NULL,
    artifact_digest text NOT NULL,
    archive_key_ciphertext text NOT NULL,
    state text NOT NULL DEFAULT 'QUEUED'
        CHECK(state IN ('QUEUED','VERIFYING','VERIFIED','FAILED','EXPIRED')),
    owner_id text,
    owner_epoch bigint NOT NULL DEFAULT 0 CHECK(owner_epoch>=0),
    lease_expires_at timestamptz,
    manifest_digest text,
    database_total integer NOT NULL CHECK(database_total BETWEEN 1 AND 1024),
    member_total integer CHECK(member_total IS NULL OR member_total>=1),
    plaintext_size bigint CHECK(plaintext_size IS NULL OR plaintext_size>=0),
    error_code text,
    verified_at timestamptz,
    expires_at timestamptz,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    UNIQUE(subject_id,idempotency_key),
    CHECK((state='VERIFYING')=(owner_id IS NOT NULL)),
    CHECK((owner_id IS NULL)=(lease_expires_at IS NULL)),
    CHECK((state IN ('VERIFIED','EXPIRED'))=(verified_at IS NOT NULL)),
    CHECK((state IN ('VERIFIED','EXPIRED'))=(expires_at IS NOT NULL)),
    CHECK((state IN ('VERIFIED','EXPIRED'))=(manifest_digest IS NOT NULL)),
    CHECK((state IN ('VERIFIED','EXPIRED'))=(member_total IS NOT NULL)),
    CHECK((state IN ('VERIFIED','EXPIRED'))=(plaintext_size IS NOT NULL)),
    CHECK((state='FAILED')=(error_code IS NOT NULL)),
    CHECK(expires_at IS NULL OR expires_at>verified_at)
);
CREATE INDEX backup_verification_resume
    ON backup_verification_jobs(state,lease_expires_at,created_at);
CREATE INDEX backup_verification_receipts
    ON backup_verification_jobs(backup_id,state,expires_at DESC);

CREATE TABLE backup_verification_events (
    event_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    verification_id text NOT NULL REFERENCES backup_verification_jobs(verification_id),
    owner_epoch bigint NOT NULL,
    step text NOT NULL,
    details jsonb NOT NULL DEFAULT '{}',
    created_at timestamptz NOT NULL DEFAULT clock_timestamp()
);
CREATE TRIGGER backup_verification_events_immutable
    BEFORE UPDATE OR DELETE ON backup_verification_events
    FOR EACH ROW EXECUTE FUNCTION forest_reject_fact_mutation();

CREATE FUNCTION forest_backup_verification_guard() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF TG_OP='DELETE'
       OR ROW(NEW.verification_id,NEW.backup_id,NEW.subject_id,NEW.idempotency_key,
              NEW.input_digest,NEW.artifact_digest,NEW.archive_key_ciphertext,
              NEW.database_total,NEW.created_at)
          IS DISTINCT FROM
          ROW(OLD.verification_id,OLD.backup_id,OLD.subject_id,OLD.idempotency_key,
              OLD.input_digest,OLD.artifact_digest,OLD.archive_key_ciphertext,
              OLD.database_total,OLD.created_at)
       OR OLD.state IN ('FAILED','EXPIRED')
       OR OLD.state='VERIFIED' AND (
           NEW.state<>'EXPIRED' OR OLD.expires_at>clock_timestamp()
           OR ROW(NEW.manifest_digest,NEW.member_total,NEW.plaintext_size,
                  NEW.verified_at,NEW.expires_at)
              IS DISTINCT FROM
              ROW(OLD.manifest_digest,OLD.member_total,OLD.plaintext_size,
                  OLD.verified_at,OLD.expires_at)
       )
       OR NEW.owner_epoch<OLD.owner_epoch THEN
        RAISE EXCEPTION 'Backup verification identity or terminal state is immutable'
            USING ERRCODE='55000';
    END IF;
    IF NOT (
        OLD.state='QUEUED' AND NEW.state IN ('VERIFYING','FAILED')
        OR OLD.state='VERIFYING' AND NEW.state IN ('VERIFYING','VERIFIED','FAILED')
        OR OLD.state='VERIFIED' AND NEW.state='EXPIRED'
    ) THEN
        RAISE EXCEPTION 'Invalid backup verification transition' USING ERRCODE='55000';
    END IF;
    IF NEW.owner_epoch=OLD.owner_epoch+1 THEN
        IF NEW.state<>'VERIFYING' THEN
            RAISE EXCEPTION 'Backup verification claim epoch is invalid' USING ERRCODE='55000';
        END IF;
    ELSIF NEW.owner_epoch<>OLD.owner_epoch THEN
        RAISE EXCEPTION 'Backup verification claim epoch is invalid' USING ERRCODE='55000';
    END IF;
    RETURN NEW;
END;
$$;
CREATE TRIGGER backup_verification_guard
    BEFORE UPDATE OR DELETE ON backup_verification_jobs
    FOR EACH ROW EXECUTE FUNCTION forest_backup_verification_guard();

ALTER TABLE restore_stage_jobs ADD COLUMN verification_id text
    REFERENCES backup_verification_jobs(verification_id);

CREATE OR REPLACE FUNCTION forest_restore_job_identity() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF TG_OP='DELETE'
       OR ROW(NEW.restore_id,NEW.backup_id,NEW.verification_id,NEW.subject_id,NEW.idempotency_key,
              NEW.input_digest,NEW.artifact_digest,NEW.archive_key_ciphertext,
              NEW.database_total,NEW.expires_at,NEW.created_at)
          IS DISTINCT FROM
          ROW(OLD.restore_id,OLD.backup_id,OLD.verification_id,OLD.subject_id,OLD.idempotency_key,
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
