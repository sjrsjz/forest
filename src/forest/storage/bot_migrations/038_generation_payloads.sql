-- Independent administrator/Host input domains are dependencies, not memory units.
CREATE TABLE public.bot_payload_domains (
 bot_id text NOT NULL REFERENCES public.forest_bot_identity(bot_id),
 kind text NOT NULL CHECK(kind IN ('SERVICE_INPUT','GENERATION_CONFIG','STATE_CONTRACT')),
 owner_id text NOT NULL CHECK(octet_length(owner_id) BETWEEN 1 AND 256 AND owner_id !~ '[[:cntrl:]]'),
 source_unit_id text NOT NULL CHECK(source_unit_id ~ '^domain:sha256:[a-f0-9]{64}$'),
 created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
 PRIMARY KEY(bot_id,kind,owner_id),
 UNIQUE(bot_id,source_unit_id)
);
CREATE TRIGGER bot_payload_domains_immutable BEFORE UPDATE OR DELETE ON public.bot_payload_domains
 FOR EACH ROW EXECUTE FUNCTION public.forest_bot_reject_mutation();

ALTER TABLE public.bot_generations
 ADD COLUMN config_payload_id text,
 ADD COLUMN config_payload_digest text CHECK(config_payload_digest ~ '^sha256:[a-f0-9]{64}$'),
 ADD FOREIGN KEY(bot_id,config_payload_id) REFERENCES public.bot_payload_headers(bot_id,payload_id),
 ALTER COLUMN config DROP NOT NULL,
 ALTER COLUMN config DROP DEFAULT,
 ALTER COLUMN state_value_schema DROP NOT NULL,
 ALTER COLUMN state_value_schema DROP DEFAULT;
DROP TRIGGER bot_generation_identity ON public.bot_generations;
DROP TRIGGER bot_generation_state_identity ON public.bot_generations;
-- Frozen Python conversion restores identity triggers and strict NULL constraints.

-- Preserve 025 source qualification while recognizing only exact registered
-- Host domains as non-recallable dependencies. Conversation authorization still
-- belongs to MemorySourceStore; a domain never turns into a selectable unit.
CREATE OR REPLACE FUNCTION public.forest_graph_source_eligible(target_bot text,target_unit text)
RETURNS boolean LANGUAGE sql STABLE AS $$
    SELECT EXISTS(
        SELECT 1 FROM public.bot_memory_source_units u
        JOIN public.bot_payload_headers h USING(bot_id,payload_id)
        WHERE u.bot_id=target_bot AND u.source_unit_id=target_unit
          AND u.lineage_complete AND h.state='ACTIVE'
          AND EXISTS(SELECT 1 FROM public.bot_payload_bodies b WHERE b.bot_id=u.bot_id AND b.payload_id=u.payload_id)
          AND (
            EXISTS(SELECT 1 FROM public.bot_payload_bodies b,
                jsonb_array_elements(convert_from(b.content,'UTF8')::jsonb->'parts') part
                WHERE b.bot_id=u.bot_id AND b.payload_id=u.payload_id AND part->>'type'<>'image')
            OR EXISTS(SELECT 1 FROM public.bot_memory_media_heads m JOIN public.bot_payload_headers mh USING(bot_id,payload_id)
                WHERE m.bot_id=u.bot_id AND m.source_unit_id=u.source_unit_id AND m.status='CAPTURED' AND mh.state='ACTIVE'
                  AND EXISTS(SELECT 1 FROM public.bot_payload_bodies b WHERE b.bot_id=mh.bot_id AND b.payload_id=mh.payload_id)
                  AND NOT EXISTS(SELECT 1 FROM public.bot_payload_sources p JOIN public.bot_payload_suppression s
                    ON s.bot_id=p.bot_id AND s.source_unit_id=p.source_unit_id
                    WHERE p.bot_id=mh.bot_id AND p.payload_id=mh.payload_id
                      AND (s.part_id='*' OR p.part_id='*' OR s.part_id=p.part_id)))
          )
          AND NOT EXISTS(
            SELECT 1 FROM public.bot_payload_sources p
            LEFT JOIN public.bot_memory_source_units d ON d.bot_id=p.bot_id AND d.source_unit_id=p.source_unit_id
            LEFT JOIN public.bot_payload_headers dh ON dh.bot_id=d.bot_id AND dh.payload_id=d.payload_id
            LEFT JOIN public.bot_payload_domains domain ON domain.bot_id=p.bot_id
              AND domain.source_unit_id=p.source_unit_id AND p.source_revision=1 AND p.part_id='*'
            WHERE p.bot_id=u.bot_id AND p.payload_id=u.payload_id AND (
              (domain.source_unit_id IS NULL AND (
                d.source_unit_id IS NULL OR NOT d.lineage_complete OR d.source_revision<>p.source_revision OR dh.state<>'ACTIVE'
                OR NOT EXISTS(SELECT 1 FROM public.bot_payload_bodies b WHERE b.bot_id=d.bot_id AND b.payload_id=d.payload_id)
                OR NOT EXISTS(SELECT 1 FROM public.bot_memory_source_occurrences o
                    JOIN public.bot_memory_source_eligibility e USING(bot_id,occurrence_id)
                    WHERE o.bot_id=d.bot_id AND o.source_unit_id=d.source_unit_id)
                OR EXISTS(SELECT 1 FROM public.bot_memory_source_occurrences o
                    JOIN public.bot_message_inbox i USING(bot_id,event_id)
                    LEFT JOIN public.bot_conversation_triggers t ON t.bot_id=i.bot_id AND t.event_id=i.event_id
                    WHERE o.bot_id=d.bot_id AND o.source_unit_id=d.source_unit_id
                      AND (i.context_disposition<>'READABLE' OR (i.origin='job' AND t.status='CANCELLED')))
                OR EXISTS(SELECT 1 FROM public.bot_memory_source_occurrences o
                    JOIN public.bot_message_echo_aliases a USING(bot_id,event_id)
                    WHERE o.bot_id=d.bot_id AND o.source_unit_id=d.source_unit_id)
              ))
              OR EXISTS(SELECT 1 FROM public.bot_payload_suppression s WHERE s.bot_id=p.bot_id
                  AND s.source_unit_id=p.source_unit_id AND (s.part_id='*' OR p.part_id='*' OR s.part_id=p.part_id))
            )
          )
          AND (
            SELECT count(*)<=4096 AND NOT COALESCE(bool_or(EXISTS(
              SELECT 1 FROM public.bot_payload_suppression s WHERE s.bot_id=u.bot_id
                AND s.source_unit_id=p.source_unit_id AND (s.part_id='*' OR p.part_id='*' OR s.part_id=p.part_id)
            )),false)
            FROM public.forest_source_dependency_closure(u.bot_id,h.manifest->'sources',4096) p
          )
    )
$$;
