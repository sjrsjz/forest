-- Content bodies can be removed without destroying operation identity or lineage.
-- This migration does not yet move legacy 001--020 plaintext into these tables.
CREATE TABLE public.bot_memory_clock (
    bot_id text PRIMARY KEY REFERENCES public.forest_bot_identity(bot_id),
    deletion_epoch bigint NOT NULL DEFAULT 0 CHECK(deletion_epoch>=0),
    fact_revision bigint NOT NULL DEFAULT 0 CHECK(fact_revision>=0)
);
CREATE TABLE public.bot_payload_forget_requests (
    bot_id text NOT NULL REFERENCES public.forest_bot_identity(bot_id),
    operation_id text NOT NULL, input_digest text NOT NULL, targets jsonb NOT NULL,
    deletion_epoch bigint NOT NULL CHECK(deletion_epoch>0),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY(bot_id,operation_id), UNIQUE(bot_id,deletion_epoch)
);
CREATE TABLE public.bot_payload_suppression (
    bot_id text NOT NULL, source_unit_id text NOT NULL, part_id text NOT NULL,
    operation_id text NOT NULL, deletion_epoch bigint NOT NULL,
    PRIMARY KEY(bot_id,source_unit_id,part_id),
    FOREIGN KEY(bot_id,operation_id) REFERENCES public.bot_payload_forget_requests(bot_id,operation_id)
);
CREATE TABLE public.bot_payload_headers (
    bot_id text NOT NULL REFERENCES public.forest_bot_identity(bot_id), payload_id text NOT NULL,
    owner_kind text NOT NULL, owner_id text NOT NULL, encoding text NOT NULL CHECK(encoding IN ('json','bytes')),
    digest text NOT NULL, byte_length bigint NOT NULL CHECK(byte_length BETWEEN 0 AND 33554432),
    input_digest text NOT NULL, manifest jsonb NOT NULL CHECK(
        jsonb_typeof(manifest)='object' AND manifest ?& ARRAY['sources','parents']
        AND jsonb_typeof(manifest->'sources')='array' AND jsonb_typeof(manifest->'parents')='array'
        AND jsonb_array_length(manifest->'sources')<=4096 AND jsonb_array_length(manifest->'parents')<=256),
    accepted_deletion_epoch bigint NOT NULL CHECK(accepted_deletion_epoch>=0),
    state text NOT NULL DEFAULT 'ACTIVE' CHECK(state IN ('ACTIVE','REVOKED','SCRUBBED')),
    forget_operation_id text, created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY(bot_id,payload_id), UNIQUE(bot_id,owner_kind,owner_id),
    FOREIGN KEY(bot_id,forget_operation_id) REFERENCES public.bot_payload_forget_requests(bot_id,operation_id),
    CHECK((state='ACTIVE')=(forget_operation_id IS NULL))
);
CREATE INDEX bot_payload_scrub_pending ON public.bot_payload_headers(bot_id,payload_id) WHERE state='REVOKED';
CREATE TABLE public.bot_payload_sources (
    bot_id text NOT NULL, payload_id text NOT NULL, source_unit_id text NOT NULL,
    source_revision bigint NOT NULL CHECK(source_revision>0), part_id text NOT NULL,
    PRIMARY KEY(bot_id,payload_id,source_unit_id,source_revision,part_id),
    FOREIGN KEY(bot_id,payload_id) REFERENCES public.bot_payload_headers(bot_id,payload_id)
);
CREATE INDEX bot_payload_source_closure ON public.bot_payload_sources(bot_id,source_unit_id,part_id,payload_id);
CREATE TABLE public.bot_payload_derivations (
    bot_id text NOT NULL, payload_id text NOT NULL, parent_payload_id text NOT NULL,
    PRIMARY KEY(bot_id,payload_id,parent_payload_id), CHECK(payload_id<>parent_payload_id),
    FOREIGN KEY(bot_id,payload_id) REFERENCES public.bot_payload_headers(bot_id,payload_id),
    FOREIGN KEY(bot_id,parent_payload_id) REFERENCES public.bot_payload_headers(bot_id,payload_id)
);
CREATE TABLE public.bot_payload_bodies (
    bot_id text NOT NULL, payload_id text NOT NULL, content bytea NOT NULL,
    PRIMARY KEY(bot_id,payload_id),
    FOREIGN KEY(bot_id,payload_id) REFERENCES public.bot_payload_headers(bot_id,payload_id)
);
CREATE TRIGGER bot_payload_forget_immutable BEFORE UPDATE OR DELETE ON public.bot_payload_forget_requests
    FOR EACH ROW EXECUTE FUNCTION public.forest_bot_reject_mutation();
CREATE TRIGGER bot_payload_suppression_immutable BEFORE UPDATE OR DELETE ON public.bot_payload_suppression
    FOR EACH ROW EXECUTE FUNCTION public.forest_bot_reject_mutation();
CREATE TRIGGER bot_payload_sources_immutable BEFORE UPDATE OR DELETE ON public.bot_payload_sources
    FOR EACH ROW EXECUTE FUNCTION public.forest_bot_reject_mutation();
CREATE TRIGGER bot_payload_derivations_immutable BEFORE UPDATE OR DELETE ON public.bot_payload_derivations
    FOR EACH ROW EXECUTE FUNCTION public.forest_bot_reject_mutation();
CREATE FUNCTION public.forest_payload_header_guard() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF TG_OP='DELETE' OR (to_jsonb(NEW)-ARRAY['state','forget_operation_id']) IS DISTINCT FROM
                           (to_jsonb(OLD)-ARRAY['state','forget_operation_id']) THEN
        RAISE EXCEPTION 'Payload identity and manifest are immutable' USING ERRCODE='55000';
    END IF;
    IF NEW IS NOT DISTINCT FROM OLD THEN RETURN NEW; END IF;
    IF OLD.state='ACTIVE' AND NEW.state='REVOKED' AND EXISTS(
        SELECT 1 FROM public.bot_payload_forget_requests f,
             jsonb_array_elements(f.targets) t,
             public.bot_payload_sources s
        WHERE f.bot_id=NEW.bot_id AND f.operation_id=NEW.forget_operation_id
          AND s.bot_id=NEW.bot_id AND s.payload_id=NEW.payload_id
          AND s.source_unit_id=t->>'source_unit_id'
          AND (s.part_id='*' OR t->>'part_id'='*' OR s.part_id=t->>'part_id')
    ) THEN RETURN NEW; END IF;
    IF OLD.state='REVOKED' AND NEW.state='SCRUBBED'
       AND NEW.forget_operation_id=OLD.forget_operation_id AND NOT EXISTS(
           SELECT 1 FROM public.bot_payload_bodies WHERE bot_id=NEW.bot_id AND payload_id=NEW.payload_id
       ) THEN RETURN NEW; END IF;
    RAISE EXCEPTION 'Payload state cannot be revived or scrubbed without a request' USING ERRCODE='55000';
END $$;
CREATE TRIGGER bot_payload_header_guard BEFORE UPDATE OR DELETE ON public.bot_payload_headers
    FOR EACH ROW EXECUTE FUNCTION public.forest_payload_header_guard();
CREATE FUNCTION public.forest_payload_body_guard() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE h public.bot_payload_headers; c public.bot_memory_clock;
BEGIN
    IF TG_OP='UPDATE' THEN
        RAISE EXCEPTION 'Payload bodies are write once' USING ERRCODE='55000';
    END IF;
    SELECT * INTO c FROM public.bot_memory_clock WHERE bot_id=COALESCE(NEW.bot_id,OLD.bot_id) FOR UPDATE;
    SELECT * INTO h FROM public.bot_payload_headers
        WHERE bot_id=COALESCE(NEW.bot_id,OLD.bot_id) AND payload_id=COALESCE(NEW.payload_id,OLD.payload_id);
    IF TG_OP='DELETE' THEN
        IF h.state='REVOKED' AND h.forget_operation_id IS NOT NULL THEN RETURN OLD; END IF;
        RAISE EXCEPTION 'Payload removal requires a committed revocation' USING ERRCODE='55000';
    END IF;
    IF c.bot_id IS NULL OR h.payload_id IS NULL OR h.state<>'ACTIVE' OR h.accepted_deletion_epoch<>c.deletion_epoch
       OR h.byte_length<>octet_length(NEW.content)
       OR h.digest<>'sha256:' || encode(sha256(NEW.content),'hex')
       OR (SELECT count(*) FROM public.bot_payload_sources s WHERE s.bot_id=h.bot_id AND s.payload_id=h.payload_id)
          <>jsonb_array_length(h.manifest->'sources')
       OR (SELECT count(*) FROM public.bot_payload_derivations d WHERE d.bot_id=h.bot_id AND d.payload_id=h.payload_id)
          <>jsonb_array_length(h.manifest->'parents')
       OR EXISTS(SELECT 1 FROM public.bot_payload_sources s JOIN public.bot_payload_suppression x
           ON x.bot_id=s.bot_id AND x.source_unit_id=s.source_unit_id
           AND (x.part_id='*' OR s.part_id='*' OR x.part_id=s.part_id)
           WHERE s.bot_id=h.bot_id AND s.payload_id=h.payload_id)
       OR EXISTS(SELECT 1 FROM public.bot_payload_derivations d JOIN public.bot_payload_headers p
           ON p.bot_id=d.bot_id AND p.payload_id=d.parent_payload_id
           WHERE d.bot_id=h.bot_id AND d.payload_id=h.payload_id AND (p.state<>'ACTIVE' OR NOT EXISTS(
               SELECT 1 FROM public.bot_payload_bodies b WHERE b.bot_id=p.bot_id AND b.payload_id=p.payload_id)))
       OR EXISTS(SELECT 1 FROM public.bot_payload_derivations d JOIN public.bot_payload_sources ps
           ON ps.bot_id=d.bot_id AND ps.payload_id=d.parent_payload_id
           WHERE d.bot_id=h.bot_id AND d.payload_id=h.payload_id AND NOT EXISTS(
               SELECT 1 FROM public.bot_payload_sources cs WHERE cs.bot_id=h.bot_id AND cs.payload_id=h.payload_id
                 AND cs.source_unit_id=ps.source_unit_id AND cs.source_revision=ps.source_revision AND cs.part_id=ps.part_id)) THEN
        RAISE EXCEPTION 'Payload content has no current admission' USING ERRCODE='55000';
    END IF;
    RETURN NEW;
END $$;
CREATE TRIGGER bot_payload_body_guard BEFORE INSERT OR UPDATE OR DELETE ON public.bot_payload_bodies
    FOR EACH ROW EXECUTE FUNCTION public.forest_payload_body_guard();
CREATE FUNCTION public.forest_payload_lineage_guard() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE h public.bot_payload_headers;
BEGIN
    SELECT * INTO h FROM public.bot_payload_headers WHERE bot_id=NEW.bot_id AND payload_id=NEW.payload_id;
    IF h.state<>'ACTIVE' OR EXISTS(SELECT 1 FROM public.bot_payload_bodies
        WHERE bot_id=NEW.bot_id AND payload_id=NEW.payload_id) THEN
        RAISE EXCEPTION 'Payload lineage is frozen before body admission' USING ERRCODE='55000';
    END IF;
    IF TG_TABLE_NAME='bot_payload_sources' THEN
        IF NOT (h.manifest->'sources' @> jsonb_build_array(jsonb_build_object(
            'source_unit_id',NEW.source_unit_id,'source_revision',NEW.source_revision,'part_id',NEW.part_id))) THEN
            RAISE EXCEPTION 'Payload source differs from manifest' USING ERRCODE='55000';
        END IF;
    ELSE
        IF NOT (h.manifest->'parents' @> jsonb_build_array(NEW.parent_payload_id)) THEN
            RAISE EXCEPTION 'Payload parent differs from manifest' USING ERRCODE='55000';
        END IF;
    END IF;
    RETURN NEW;
END $$;
CREATE TRIGGER bot_payload_source_guard BEFORE INSERT ON public.bot_payload_sources
    FOR EACH ROW EXECUTE FUNCTION public.forest_payload_lineage_guard();
CREATE TRIGGER bot_payload_derivation_guard BEFORE INSERT ON public.bot_payload_derivations
    FOR EACH ROW EXECUTE FUNCTION public.forest_payload_lineage_guard();
