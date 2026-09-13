-- Background capture is host-owned and independent of Agent/model capability.
-- Only immutable metadata lives here; original bytes belong to PayloadStore.
CREATE TABLE public.bot_memory_media_heads (
    bot_id text NOT NULL, capture_id text NOT NULL,
    source_unit_id text NOT NULL, source_revision bigint NOT NULL CHECK(source_revision=1),
    part_id text NOT NULL, media_ref text NOT NULL,
    status text NOT NULL DEFAULT 'PENDING' CHECK(status IN
        ('PENDING','CLAIMED','CAPTURED','RETRYABLE','UNAVAILABLE','REJECTED','QUOTA_EXCEEDED','REVOKED')),
    attempt_count integer NOT NULL DEFAULT 0 CHECK(attempt_count>=0),
    claim_epoch bigint NOT NULL DEFAULT 0 CHECK(claim_epoch>=0), active_attempt_id text,
    config_revision bigint NOT NULL DEFAULT 0, next_attempt_at timestamptz,
    error_code text, payload_id text, blob_digest text, mime text,
    width integer, height integer, byte_length bigint,
    storage_accounted boolean NOT NULL DEFAULT false,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY(bot_id,capture_id), UNIQUE(bot_id,source_unit_id,part_id),
    FOREIGN KEY(bot_id,source_unit_id) REFERENCES public.bot_memory_source_units(bot_id,source_unit_id),
    FOREIGN KEY(bot_id,payload_id) REFERENCES public.bot_payload_headers(bot_id,payload_id),
    CHECK((payload_id IS NULL)=(byte_length IS NULL)),
    CHECK(NOT storage_accounted OR byte_length>0)
);
CREATE TABLE public.bot_memory_media_attempts (
    bot_id text NOT NULL, attempt_id text NOT NULL, capture_id text NOT NULL,
    claim_epoch bigint NOT NULL CHECK(claim_epoch>0), owner_id text NOT NULL,
    host_id text NOT NULL, host_incarnation text NOT NULL, host_owner_epoch bigint NOT NULL,
    storage_generation bigint NOT NULL, maintenance_epoch bigint NOT NULL,
    config_revision bigint NOT NULL, deletion_epoch bigint NOT NULL,
    source_unit_id text NOT NULL, source_revision bigint NOT NULL, part_id text NOT NULL,
    occurrence_id text NOT NULL, inbound_media_ref text NOT NULL, source_digest text NOT NULL,
    origin text NOT NULL, deadline_at timestamptz NOT NULL, budget_day date NOT NULL,
    reserved_download_bytes bigint NOT NULL CHECK(reserved_download_bytes>0),
    reserved_storage_bytes bigint NOT NULL CHECK(reserved_storage_bytes>0),
    status text NOT NULL DEFAULT 'CLAIMED' CHECK(status IN
        ('CLAIMED','DISPATCHING','SUCCEEDED','RETRYABLE','REJECTED','REVOKED','ABANDONED')),
    quota_settled boolean NOT NULL DEFAULT false, charged_bytes bigint,
    error_code text, completed_at timestamptz,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY(bot_id,attempt_id), UNIQUE(bot_id,capture_id,claim_epoch),
    FOREIGN KEY(bot_id,capture_id) REFERENCES public.bot_memory_media_heads(bot_id,capture_id),
    FOREIGN KEY(bot_id,occurrence_id) REFERENCES public.bot_memory_source_occurrences(bot_id,occurrence_id),
    FOREIGN KEY(bot_id,inbound_media_ref) REFERENCES public.bot_inbound_media_refs(bot_id,media_ref)
);
CREATE TABLE public.bot_memory_media_quota (
    bot_id text PRIMARY KEY REFERENCES public.forest_bot_identity(bot_id),
    retained_bytes bigint NOT NULL DEFAULT 0 CHECK(retained_bytes>=0),
    reserved_bytes bigint NOT NULL DEFAULT 0 CHECK(reserved_bytes>=0)
);
CREATE TABLE public.bot_memory_media_daily (
    bot_id text NOT NULL REFERENCES public.forest_bot_identity(bot_id), budget_day date NOT NULL,
    charged_bytes bigint NOT NULL DEFAULT 0 CHECK(charged_bytes>=0),
    PRIMARY KEY(bot_id,budget_day)
);
CREATE INDEX bot_memory_media_pending ON public.bot_memory_media_heads(bot_id,status,next_attempt_at,capture_id);
CREATE FUNCTION public.forest_memory_media_head_guard() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF TG_OP='DELETE' OR ROW(NEW.bot_id,NEW.capture_id,NEW.source_unit_id,NEW.source_revision,NEW.part_id,NEW.media_ref,NEW.created_at)
       IS DISTINCT FROM ROW(OLD.bot_id,OLD.capture_id,OLD.source_unit_id,OLD.source_revision,OLD.part_id,OLD.media_ref,OLD.created_at)
       OR (OLD.payload_id IS NOT NULL AND ROW(NEW.payload_id,NEW.blob_digest,NEW.mime,NEW.width,NEW.height,NEW.byte_length)
           IS DISTINCT FROM ROW(OLD.payload_id,OLD.blob_digest,OLD.mime,OLD.width,OLD.height,OLD.byte_length)) THEN
        RAISE EXCEPTION 'Memory media identity and captured pixels are fixed' USING ERRCODE='55000';
    END IF;
    RETURN NEW;
END $$;
CREATE TRIGGER bot_memory_media_head_guard BEFORE UPDATE OR DELETE ON public.bot_memory_media_heads
    FOR EACH ROW EXECUTE FUNCTION public.forest_memory_media_head_guard();
CREATE TRIGGER bot_memory_media_attempt_identity BEFORE UPDATE OR DELETE ON public.bot_memory_media_attempts
    FOR EACH ROW EXECUTE FUNCTION public.forest_agent_fixed_fields('{status,quota_settled,charged_bytes,error_code,completed_at}');
