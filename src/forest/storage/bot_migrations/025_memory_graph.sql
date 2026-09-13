-- Host-owned bounded graph shards. Published records never contain value bodies.
CREATE TABLE public.bot_memory_graph_clock (
    bot_id text PRIMARY KEY REFERENCES public.forest_bot_identity(bot_id),
    graph_revision bigint NOT NULL DEFAULT 0 CHECK(graph_revision>=0)
);
CREATE TABLE public.bot_memory_graph_scopes (
    bot_id text NOT NULL, scope_id text NOT NULL, windows jsonb NOT NULL,
    revision bigint NOT NULL DEFAULT 0, operation_revision bigint NOT NULL DEFAULT 0,
    PRIMARY KEY(bot_id,scope_id),
    FOREIGN KEY(bot_id) REFERENCES public.forest_bot_identity(bot_id)
);
CREATE TABLE public.bot_memory_graph_operations (
    bot_id text NOT NULL, operation_id text NOT NULL, scope_id text NOT NULL,
    fact_id text NOT NULL, kind text NOT NULL CHECK(kind IN ('assertion','relation','retraction')),
    metadata jsonb NOT NULL, input_digest text NOT NULL,
    PRIMARY KEY(bot_id,operation_id), UNIQUE(bot_id,fact_id),
    FOREIGN KEY(bot_id,scope_id) REFERENCES public.bot_memory_graph_scopes(bot_id,scope_id)
);
CREATE TABLE public.bot_memory_graph_publications (
    bot_id text NOT NULL, scope_id text NOT NULL, revision bigint NOT NULL,
    graph_revision bigint NOT NULL, operation_id text NOT NULL, input_digest text NOT NULL,
    snapshot jsonb NOT NULL, projection_digest text NOT NULL, canonical_projection bytea NOT NULL,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY(bot_id,scope_id,revision), UNIQUE(bot_id,graph_revision), UNIQUE(bot_id,operation_id),
    FOREIGN KEY(bot_id,scope_id) REFERENCES public.bot_memory_graph_scopes(bot_id,scope_id)
);
CREATE INDEX bot_memory_graph_retraction_targets ON public.bot_memory_graph_operations(bot_id,(metadata->>'target_id')) WHERE kind='retraction';
CREATE INDEX bot_memory_graph_global_view ON public.bot_memory_graph_publications(bot_id,scope_id,graph_revision DESC);
CREATE TABLE public.bot_memory_graph_nodes (
    bot_id text NOT NULL, scope_id text NOT NULL, revision bigint NOT NULL,
    node_id text NOT NULL, metadata jsonb NOT NULL,
    PRIMARY KEY(bot_id,scope_id,revision,node_id),
    FOREIGN KEY(bot_id,scope_id,revision) REFERENCES public.bot_memory_graph_publications(bot_id,scope_id,revision)
);
CREATE TABLE public.bot_memory_graph_edges (
    bot_id text NOT NULL, scope_id text NOT NULL, revision bigint NOT NULL,
    edge_id text NOT NULL, from_node_id text NOT NULL, to_node_id text NOT NULL, metadata jsonb NOT NULL,
    PRIMARY KEY(bot_id,scope_id,revision,edge_id),
    FOREIGN KEY(bot_id,scope_id,revision) REFERENCES public.bot_memory_graph_publications(bot_id,scope_id,revision)
);
CREATE INDEX bot_memory_graph_from ON public.bot_memory_graph_edges(bot_id,scope_id,revision,from_node_id,edge_id);
CREATE INDEX bot_memory_graph_to ON public.bot_memory_graph_edges(bot_id,scope_id,revision,to_node_id,edge_id);
CREATE TRIGGER bot_memory_graph_scope_identity BEFORE UPDATE OR DELETE ON public.bot_memory_graph_scopes
    FOR EACH ROW EXECUTE FUNCTION public.forest_agent_fixed_fields('{revision,operation_revision}');
CREATE TRIGGER bot_memory_graph_operations_immutable BEFORE UPDATE OR DELETE ON public.bot_memory_graph_operations
    FOR EACH ROW EXECUTE FUNCTION public.forest_bot_reject_mutation();
CREATE TRIGGER bot_memory_graph_publications_immutable BEFORE UPDATE OR DELETE ON public.bot_memory_graph_publications
    FOR EACH ROW EXECUTE FUNCTION public.forest_bot_reject_mutation();
CREATE TRIGGER bot_memory_graph_nodes_immutable BEFORE UPDATE OR DELETE ON public.bot_memory_graph_nodes
    FOR EACH ROW EXECUTE FUNCTION public.forest_bot_reject_mutation();
CREATE TRIGGER bot_memory_graph_edges_immutable BEFORE UPDATE OR DELETE ON public.bot_memory_graph_edges
    FOR EACH ROW EXECUTE FUNCTION public.forest_bot_reject_mutation();

-- Selection only, never conversation authorization. The repository also calls
-- MemorySourceStore.authorize_sources_in on the selected bounded set. This SQL
-- permits an indexed nearest qualified predecessor across arbitrarily long
-- empty/deleted stretches, without loading the Bot's history into Python.
CREATE FUNCTION public.forest_graph_source_eligible(target_bot text,target_unit text)
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
            WHERE p.bot_id=u.bot_id AND p.payload_id=u.payload_id AND (
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
              OR EXISTS(SELECT 1 FROM public.bot_payload_suppression s WHERE s.bot_id=p.bot_id
                  AND s.source_unit_id=p.source_unit_id AND (s.part_id='*' OR p.part_id='*' OR s.part_id=p.part_id))
            )
          )
    )
$$;
