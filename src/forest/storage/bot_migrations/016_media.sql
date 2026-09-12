ALTER TABLE public.bot_inbound_media_refs
    ADD COLUMN capture_epoch bigint NOT NULL DEFAULT 0,
    ADD COLUMN capture_id text,
    ADD COLUMN capture_expires_at timestamptz,
    ADD COLUMN retry_after timestamptz,
    ADD COLUMN error_code text;

CREATE TABLE public.bot_media_captures (
    bot_id text NOT NULL,
    media_ref text NOT NULL,
    media_revision bigint NOT NULL CHECK(media_revision = 1),
    event_id text NOT NULL,
    part_index integer NOT NULL CHECK(part_index >= 0),
    content bytea NOT NULL CHECK(octet_length(content) BETWEEN 1 AND 8388608),
    blob_digest text NOT NULL CHECK(blob_digest = 'sha256:' || encode(sha256(content), 'hex')),
    mime text NOT NULL CHECK(mime IN ('image/png','image/jpeg','image/webp','image/gif')),
    width integer NOT NULL CHECK(width BETWEEN 1 AND 16384),
    height integer NOT NULL CHECK(height BETWEEN 1 AND 16384),
    frame_count integer NOT NULL CHECK(frame_count BETWEEN 1 AND 100),
    decoded_pixels bigint NOT NULL CHECK(decoded_pixels BETWEEN 1 AND 16777216),
    source_manifest jsonb NOT NULL,
    source_digest text NOT NULL,
    capture_id text NOT NULL,
    capture_epoch bigint NOT NULL,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY(bot_id,media_ref,media_revision),
    FOREIGN KEY(bot_id,media_ref) REFERENCES public.bot_inbound_media_refs(bot_id,media_ref),
    FOREIGN KEY(bot_id,event_id,part_index) REFERENCES public.bot_inbound_media_refs(bot_id,event_id,part_index)
);
CREATE TRIGGER bot_media_captures_immutable BEFORE UPDATE OR DELETE ON public.bot_media_captures
    FOR EACH ROW EXECUTE FUNCTION public.forest_bot_reject_mutation();

CREATE TABLE public.bot_media_quota_heads (
    bot_id text PRIMARY KEY REFERENCES public.forest_bot_identity(bot_id),
    retained_bytes bigint NOT NULL DEFAULT 0 CHECK(retained_bytes BETWEEN 0 AND 536870912)
);

CREATE FUNCTION public.forest_media_source_immutable() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF TG_OP = 'DELETE' OR ROW(NEW.bot_id,NEW.media_ref,NEW.event_id,NEW.part_index,NEW.source)
       IS DISTINCT FROM ROW(OLD.bot_id,OLD.media_ref,OLD.event_id,OLD.part_index,OLD.source) THEN
        RAISE EXCEPTION 'Inbound media source is immutable' USING ERRCODE='55000';
    END IF;
    RETURN NEW;
END $$;
CREATE TRIGGER bot_media_source_immutable BEFORE UPDATE OR DELETE ON public.bot_inbound_media_refs
    FOR EACH ROW EXECUTE FUNCTION public.forest_media_source_immutable();
