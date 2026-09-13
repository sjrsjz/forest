-- Actual Worker deliveries are immutable facts. No legacy body is imported here.
CREATE TABLE public.bot_generation_exposure_heads (
 bot_id text NOT NULL,
 generation_id text NOT NULL,
 component_id text NOT NULL,
 instance_id text NOT NULL,
 worker_incarnation text NOT NULL,
 host_incarnation text NOT NULL,
 host_owner_epoch bigint NOT NULL,
 revision bigint NOT NULL DEFAULT 0 CHECK(revision>=0),
 payload_count integer NOT NULL DEFAULT 0 CHECK(payload_count BETWEEN 0 AND 256),
 status text NOT NULL DEFAULT 'LIVE' CHECK(status IN ('LIVE','REVOKED','EXHAUSTED')),
 reason_code text,
 invalidated_at timestamptz,
 PRIMARY KEY(bot_id,generation_id),
 FOREIGN KEY(bot_id,generation_id) REFERENCES public.bot_generations(bot_id,generation_id),
 CHECK((status='LIVE' AND reason_code IS NULL AND invalidated_at IS NULL) OR
       (status='EXHAUSTED' AND reason_code IN ('CAPACITY','SOURCE_CAPACITY') AND invalidated_at IS NOT NULL) OR
       (status='REVOKED' AND reason_code IN ('CONTENT_UNAVAILABLE','SOURCE_SCOPE_LIMIT') AND invalidated_at IS NOT NULL))
);
INSERT INTO public.bot_generation_exposure_heads(bot_id,generation_id,component_id,instance_id,
 worker_incarnation,host_incarnation,host_owner_epoch)
 SELECT bot_id,generation_id,component_id,instance_id,worker_incarnation,host_incarnation,host_owner_epoch
 FROM public.bot_generations;

CREATE FUNCTION public.forest_exposure_refs_valid(refs text[]) RETURNS boolean
 LANGUAGE sql IMMUTABLE AS $$
 SELECT COALESCE(array_ndims(refs)=1 AND cardinality(refs) BETWEEN 1 AND 256
  AND NOT EXISTS(SELECT 1 FROM unnest(refs) r WHERE r IS NULL OR octet_length(r) NOT BETWEEN 1 AND 256 OR r ~ '[[:cntrl:]]')
  AND refs=ARRAY(SELECT DISTINCT r FROM unnest(refs) r ORDER BY r),false)
$$;
CREATE TABLE public.bot_generation_exposures (
 bot_id text NOT NULL,
 generation_id text NOT NULL,
 exposure_id text NOT NULL CHECK(octet_length(exposure_id) BETWEEN 1 AND 256 AND exposure_id !~ '[[:cntrl:]]'),
 revision bigint NOT NULL CHECK(revision>0),
 kind text NOT NULL CHECK(kind IN ('CALL_INPUT','STATE_READ','CHILD_RESULT','CONFIG','PREPARE_STATE','MIGRATION_SOURCE','RESTORE_STATE')),
 subject_ref text NOT NULL CHECK(octet_length(subject_ref) BETWEEN 1 AND 256 AND subject_ref !~ '[[:cntrl:]]'),
 payload_refs text[] NOT NULL CHECK(public.forest_exposure_refs_valid(payload_refs)),
 input_digest text NOT NULL CHECK(input_digest ~ '^sha256:[a-f0-9]{64}$'),
 created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
 PRIMARY KEY(bot_id,generation_id,exposure_id),
 UNIQUE(bot_id,generation_id,revision),
 FOREIGN KEY(bot_id,generation_id) REFERENCES public.bot_generation_exposure_heads(bot_id,generation_id)
);
CREATE TABLE public.bot_generation_exposure_members (
 bot_id text NOT NULL,
 generation_id text NOT NULL,
 payload_id text NOT NULL,
 first_exposure_id text NOT NULL,
 PRIMARY KEY(bot_id,generation_id,payload_id),
 FOREIGN KEY(bot_id,generation_id,first_exposure_id) REFERENCES public.bot_generation_exposures(bot_id,generation_id,exposure_id),
 FOREIGN KEY(bot_id,payload_id) REFERENCES public.bot_payload_headers(bot_id,payload_id)
);
CREATE INDEX bot_generation_exposure_payload ON public.bot_generation_exposure_members(bot_id,payload_id);
CREATE TRIGGER bot_generation_exposures_immutable BEFORE UPDATE OR DELETE ON public.bot_generation_exposures
 FOR EACH ROW EXECUTE FUNCTION public.forest_bot_reject_mutation();
CREATE TRIGGER bot_generation_exposure_members_immutable BEFORE UPDATE OR DELETE ON public.bot_generation_exposure_members
 FOR EACH ROW EXECUTE FUNCTION public.forest_bot_reject_mutation();

CREATE FUNCTION public.forest_exposure_head_guard() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE g record;
BEGIN
 IF TG_OP='DELETE' THEN RAISE EXCEPTION 'EXPOSURE_IDENTITY_IMMUTABLE' USING ERRCODE='55000'; END IF;
 SELECT * INTO g FROM public.bot_generations WHERE bot_id=NEW.bot_id AND generation_id=NEW.generation_id;
 IF NOT FOUND OR ROW(NEW.component_id,NEW.instance_id,NEW.worker_incarnation,NEW.host_incarnation,NEW.host_owner_epoch)
  IS DISTINCT FROM ROW(g.component_id,g.instance_id,g.worker_incarnation,g.host_incarnation,g.host_owner_epoch) THEN
  RAISE EXCEPTION 'EXPOSURE_GENERATION_IDENTITY' USING ERRCODE='23514';
 END IF;
 IF TG_OP='INSERT' THEN
  IF NEW.revision<>0 OR NEW.payload_count<>0 OR NEW.status<>'LIVE' THEN
   RAISE EXCEPTION 'EXPOSURE_INITIAL_HEAD' USING ERRCODE='23514';
  END IF;
  RETURN NEW;
 END IF;
 IF ROW(NEW.bot_id,NEW.generation_id,NEW.component_id,NEW.instance_id,NEW.worker_incarnation,NEW.host_incarnation,NEW.host_owner_epoch)
  IS DISTINCT FROM ROW(OLD.bot_id,OLD.generation_id,OLD.component_id,OLD.instance_id,OLD.worker_incarnation,OLD.host_incarnation,OLD.host_owner_epoch) THEN
  RAISE EXCEPTION 'EXPOSURE_IDENTITY_IMMUTABLE' USING ERRCODE='55000';
 END IF;
 IF NEW.revision<>OLD.revision THEN
  IF NEW.revision<>OLD.revision+1 OR NEW.status<>OLD.status OR OLD.status='REVOKED'
   OR NOT EXISTS(SELECT 1 FROM public.bot_generation_exposures e WHERE e.bot_id=NEW.bot_id AND e.generation_id=NEW.generation_id AND e.revision=NEW.revision)
   OR NEW.payload_count<>(SELECT count(*) FROM public.bot_generation_exposure_members m WHERE m.bot_id=NEW.bot_id AND m.generation_id=NEW.generation_id)
   OR NEW.payload_count<OLD.payload_count THEN
   RAISE EXCEPTION 'EXPOSURE_REVISION_CONFLICT' USING ERRCODE='23514';
  END IF;
 ELSE
  IF NEW.payload_count<>OLD.payload_count THEN RAISE EXCEPTION 'EXPOSURE_COUNT_IMMUTABLE' USING ERRCODE='23514'; END IF;
 END IF;
 IF NEW.status<>OLD.status AND NOT ((OLD.status='LIVE' AND NEW.status IN ('EXHAUSTED','REVOKED')) OR
                                   (OLD.status='EXHAUSTED' AND NEW.status='REVOKED')) THEN
  RAISE EXCEPTION 'EXPOSURE_TERMINAL_STATE' USING ERRCODE='23514';
 END IF;
 IF NEW.status=OLD.status AND ROW(NEW.reason_code,NEW.invalidated_at) IS DISTINCT FROM ROW(OLD.reason_code,OLD.invalidated_at) THEN
  RAISE EXCEPTION 'EXPOSURE_DECISION_IMMUTABLE' USING ERRCODE='55000';
 END IF;
 RETURN NEW;
END;
$$;
CREATE TRIGGER bot_generation_exposure_head_guard BEFORE INSERT OR UPDATE OR DELETE ON public.bot_generation_exposure_heads
 FOR EACH ROW EXECUTE FUNCTION public.forest_exposure_head_guard();

CREATE FUNCTION public.forest_generation_exposure_initialize() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 INSERT INTO public.bot_generation_exposure_heads(bot_id,generation_id,component_id,instance_id,worker_incarnation,host_incarnation,host_owner_epoch)
 VALUES(NEW.bot_id,NEW.generation_id,NEW.component_id,NEW.instance_id,NEW.worker_incarnation,NEW.host_incarnation,NEW.host_owner_epoch);
 RETURN NEW;
END;
$$;
CREATE TRIGGER bot_generation_exposure_initialize AFTER INSERT ON public.bot_generations
 FOR EACH ROW EXECUTE FUNCTION public.forest_generation_exposure_initialize();

CREATE FUNCTION public.forest_exposure_member_guard() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 IF NOT EXISTS(SELECT 1 FROM public.bot_generation_exposures e WHERE e.bot_id=NEW.bot_id
  AND e.generation_id=NEW.generation_id AND e.exposure_id=NEW.first_exposure_id AND NEW.payload_id=ANY(e.payload_refs)) THEN
  RAISE EXCEPTION 'EXPOSURE_MEMBER_NOT_DELIVERED' USING ERRCODE='23514';
 END IF;
 RETURN NEW;
END;
$$;
CREATE TRIGGER bot_generation_exposure_member_guard BEFORE INSERT ON public.bot_generation_exposure_members
 FOR EACH ROW EXECUTE FUNCTION public.forest_exposure_member_guard();

CREATE FUNCTION public.forest_exposure_append() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE h record; addition integer;
BEGIN
 SELECT * INTO h FROM public.bot_generation_exposure_heads WHERE bot_id=NEW.bot_id AND generation_id=NEW.generation_id FOR UPDATE;
 IF NOT FOUND OR h.status='REVOKED' OR NEW.revision<>h.revision+1 THEN
  RAISE EXCEPTION 'EXPOSURE_APPEND_FENCED' USING ERRCODE='23514';
 END IF;
 SELECT count(*) INTO addition FROM unnest(NEW.payload_refs) r WHERE NOT EXISTS(
  SELECT 1 FROM public.bot_generation_exposure_members m WHERE m.bot_id=NEW.bot_id AND m.generation_id=NEW.generation_id AND m.payload_id=r);
 IF h.payload_count+addition>256 OR (h.status='EXHAUSTED' AND addition>0) THEN
  RAISE EXCEPTION 'EXPOSURE_CAPACITY_EXCEEDED' USING ERRCODE='23514';
 END IF;
 IF EXISTS(SELECT 1 FROM unnest(NEW.payload_refs) r LEFT JOIN public.bot_payload_headers p ON p.bot_id=NEW.bot_id AND p.payload_id=r
  WHERE p.payload_id IS NULL OR p.state<>'ACTIVE' OR NOT EXISTS(SELECT 1 FROM public.bot_payload_bodies b WHERE b.bot_id=p.bot_id AND b.payload_id=p.payload_id)) THEN
  RAISE EXCEPTION 'EXPOSURE_PAYLOAD_UNAVAILABLE' USING ERRCODE='23514';
 END IF;
 RETURN NEW;
END;
$$;
CREATE TRIGGER bot_generation_exposure_append BEFORE INSERT ON public.bot_generation_exposures
 FOR EACH ROW EXECUTE FUNCTION public.forest_exposure_append();
CREATE FUNCTION public.forest_exposure_advance() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 INSERT INTO public.bot_generation_exposure_members(bot_id,generation_id,payload_id,first_exposure_id)
  SELECT NEW.bot_id,NEW.generation_id,r,NEW.exposure_id FROM unnest(NEW.payload_refs) r ON CONFLICT DO NOTHING;
 UPDATE public.bot_generation_exposure_heads SET revision=NEW.revision,payload_count=(
  SELECT count(*) FROM public.bot_generation_exposure_members WHERE bot_id=NEW.bot_id AND generation_id=NEW.generation_id)
  WHERE bot_id=NEW.bot_id AND generation_id=NEW.generation_id;
 RETURN NEW;
END;
$$;
CREATE TRIGGER bot_generation_exposure_advance AFTER INSERT ON public.bot_generation_exposures
 FOR EACH ROW EXECUTE FUNCTION public.forest_exposure_advance();
