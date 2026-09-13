-- Durable, convergent cross-trunk semantic association maintenance.
CREATE TABLE public.bot_memory_semantic_tasks (
    bot_id text NOT NULL,
    space_id text NOT NULL,
    source_unit_id text NOT NULL,
    requested_revision bigint NOT NULL DEFAULT 1 CHECK(requested_revision>0),
    processed_revision bigint NOT NULL DEFAULT 0 CHECK(processed_revision>=0),
    outcome text,
    association_count integer NOT NULL DEFAULT 0 CHECK(association_count BETWEEN 0 AND 4),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY(bot_id,space_id,source_unit_id),
    FOREIGN KEY(bot_id,source_unit_id)
        REFERENCES public.bot_memory_source_units(bot_id,source_unit_id),
    CHECK(processed_revision<=requested_revision),
    CHECK(outcome IS NULL OR outcome IN ('CURRENT','SOURCE_UNAVAILABLE','CAPACITY_EXCEEDED'))
);

CREATE TABLE public.bot_memory_semantic_votes (
    bot_id text NOT NULL,
    space_id text NOT NULL,
    origin_source_unit_id text NOT NULL,
    neighbor_source_unit_id text NOT NULL,
    association_id text NOT NULL,
    scope_id text NOT NULL,
    revision bigint NOT NULL DEFAULT 0 CHECK(revision>=0),
    active boolean NOT NULL DEFAULT false,
    current_contribution_id text,
    score_ppm integer NOT NULL CHECK(score_ppm BETWEEN 0 AND 1000000),
    origin_facet_id text NOT NULL,
    neighbor_facet_id text NOT NULL,
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY(bot_id,space_id,origin_source_unit_id,neighbor_source_unit_id),
    FOREIGN KEY(bot_id,origin_source_unit_id)
        REFERENCES public.bot_memory_source_units(bot_id,source_unit_id),
    FOREIGN KEY(bot_id,neighbor_source_unit_id)
        REFERENCES public.bot_memory_source_units(bot_id,source_unit_id),
    FOREIGN KEY(bot_id,origin_facet_id)
        REFERENCES public.bot_memory_index_facets(bot_id,facet_id),
    FOREIGN KEY(bot_id,neighbor_facet_id)
        REFERENCES public.bot_memory_index_facets(bot_id,facet_id),
    FOREIGN KEY(bot_id,scope_id)
        REFERENCES public.bot_memory_graph_scopes(bot_id,scope_id),
    CHECK(origin_source_unit_id<>neighbor_source_unit_id),
    CHECK(active=(current_contribution_id IS NOT NULL))
);
CREATE INDEX bot_memory_semantic_association
    ON public.bot_memory_semantic_votes(bot_id,space_id,association_id)
    WHERE active;
CREATE INDEX bot_memory_semantic_incoming
    ON public.bot_memory_semantic_votes(bot_id,space_id,neighbor_source_unit_id)
    WHERE active;

CREATE TABLE public.bot_memory_semantic_vote_changes (
    bot_id text NOT NULL,
    space_id text NOT NULL,
    origin_source_unit_id text NOT NULL,
    neighbor_source_unit_id text NOT NULL,
    revision bigint NOT NULL CHECK(revision>0),
    active boolean NOT NULL,
    graph_operation_id text NOT NULL,
    contribution_id text,
    score_ppm integer NOT NULL CHECK(score_ppm BETWEEN 0 AND 1000000),
    origin_facet_id text NOT NULL,
    neighbor_facet_id text NOT NULL,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY(
        bot_id,space_id,origin_source_unit_id,neighbor_source_unit_id,revision
    ),
    UNIQUE(bot_id,graph_operation_id),
    FOREIGN KEY(bot_id,space_id,origin_source_unit_id,neighbor_source_unit_id)
        REFERENCES public.bot_memory_semantic_votes(
            bot_id,space_id,origin_source_unit_id,neighbor_source_unit_id
        ),
    CHECK(active=(contribution_id IS NOT NULL))
);

CREATE TRIGGER bot_memory_semantic_tasks_fixed
    BEFORE UPDATE OR DELETE ON public.bot_memory_semantic_tasks
    FOR EACH ROW EXECUTE FUNCTION public.forest_agent_fixed_fields(
        '{requested_revision,processed_revision,outcome,association_count,updated_at}');
CREATE TRIGGER bot_memory_semantic_votes_fixed
    BEFORE UPDATE OR DELETE ON public.bot_memory_semantic_votes
    FOR EACH ROW EXECUTE FUNCTION public.forest_agent_fixed_fields(
        '{revision,active,current_contribution_id,score_ppm,origin_facet_id,neighbor_facet_id,updated_at}');
CREATE TRIGGER bot_memory_semantic_vote_changes_immutable
    BEFORE UPDATE OR DELETE ON public.bot_memory_semantic_vote_changes
    FOR EACH ROW EXECUTE FUNCTION public.forest_bot_reject_mutation();

CREATE FUNCTION public.forest_memory_semantic_queue() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    INSERT INTO public.bot_memory_semantic_tasks(bot_id,space_id,source_unit_id)
    VALUES (NEW.bot_id,NEW.space_id,NEW.source_unit_id)
    ON CONFLICT(bot_id,space_id,source_unit_id) DO UPDATE SET
        requested_revision=bot_memory_semantic_tasks.requested_revision+1,
        updated_at=clock_timestamp();
    RETURN NEW;
END $$;
CREATE TRIGGER bot_memory_vector_semantic_queue
    AFTER INSERT ON public.bot_memory_vectors
    FOR EACH ROW EXECUTE FUNCTION public.forest_memory_semantic_queue();

-- Existing vectors are ordinary pending work after an in-place upgrade.
INSERT INTO public.bot_memory_semantic_tasks(bot_id,space_id,source_unit_id)
SELECT DISTINCT bot_id,space_id,source_unit_id FROM public.bot_memory_vectors
ON CONFLICT DO NOTHING;
