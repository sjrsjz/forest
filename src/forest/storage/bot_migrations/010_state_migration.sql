-- Durable schema migration never overwrites the source before route publication.
ALTER TABLE public.bot_generations ADD COLUMN state_schema integer NOT NULL DEFAULT 1 CHECK (state_schema > 0);
ALTER TABLE public.bot_generations ADD COLUMN state_value_schema jsonb NOT NULL DEFAULT '{}'::jsonb;
ALTER TABLE public.bot_generations ADD COLUMN state_migrations jsonb NOT NULL DEFAULT '[]'::jsonb;
CREATE FUNCTION public.forest_generation_state_identity() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF ROW(OLD.state_schema, OLD.state_value_schema, OLD.state_migrations) IS DISTINCT FROM ROW(NEW.state_schema, NEW.state_value_schema, NEW.state_migrations) THEN
        RAISE EXCEPTION 'Forest generation state contract is immutable' USING ERRCODE = '55000';
    END IF;
    RETURN NEW;
END;
$$;
CREATE TRIGGER bot_generation_state_identity BEFORE UPDATE ON public.bot_generations
    FOR EACH ROW EXECUTE FUNCTION public.forest_generation_state_identity();

CREATE TABLE public.bot_component_state_heads (
    bot_id text NOT NULL,
    component_id text NOT NULL,
    state_schema integer NOT NULL DEFAULT 1 CHECK (state_schema > 0),
    revision bigint NOT NULL DEFAULT 0 CHECK (revision >= 0),
    migration_operation_id text,
    PRIMARY KEY (bot_id, component_id),
    FOREIGN KEY (bot_id, component_id) REFERENCES public.bot_component_routes(bot_id, component_id)
);
INSERT INTO public.bot_component_state_heads(bot_id,component_id,revision)
    SELECT r.bot_id,r.component_id,COALESCE(sum(s.revision),0)
    FROM public.bot_component_routes r LEFT JOIN public.bot_component_state s USING(bot_id,component_id)
    GROUP BY r.bot_id,r.component_id;
ALTER TABLE public.bot_component_state ADD COLUMN deleted boolean NOT NULL DEFAULT false;

CREATE TABLE public.bot_state_migration_sources (
    bot_id text NOT NULL,
    operation_id text NOT NULL,
    component_id text NOT NULL,
    source_generation_id text,
    source_route_epoch bigint NOT NULL,
    source_schema integer NOT NULL,
    source_revision bigint NOT NULL,
    source_digest text NOT NULL,
    source_rows jsonb NOT NULL,
    target_schema integer NOT NULL,
    captured_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (bot_id,operation_id),
    FOREIGN KEY (bot_id,operation_id) REFERENCES public.bot_lifecycle_operations(bot_id,operation_id)
);
CREATE TABLE public.bot_state_migration_results (
    bot_id text NOT NULL,
    operation_id text NOT NULL,
    source_digest text NOT NULL,
    result_digest text NOT NULL,
    result_values jsonb NOT NULL,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (bot_id,operation_id),
    FOREIGN KEY (bot_id,operation_id) REFERENCES public.bot_state_migration_sources(bot_id,operation_id)
);
CREATE TRIGGER bot_state_migration_sources_immutable BEFORE UPDATE OR DELETE ON public.bot_state_migration_sources
    FOR EACH ROW EXECUTE FUNCTION public.forest_bot_reject_mutation();
CREATE TRIGGER bot_state_migration_results_immutable BEFORE UPDATE OR DELETE ON public.bot_state_migration_results
    FOR EACH ROW EXECUTE FUNCTION public.forest_bot_reject_mutation();

-- A removed key retains its revision fence. Migration revisions carry a lifecycle
-- receipt, while ordinary revisions retain their original immutable call receipt.
ALTER TABLE public.bot_state_revisions ALTER COLUMN call_id DROP NOT NULL;
ALTER TABLE public.bot_state_revisions ADD COLUMN migration_operation_id text;
ALTER TABLE public.bot_state_revisions ADD COLUMN deleted boolean NOT NULL DEFAULT false;
ALTER TABLE public.bot_state_revisions ADD CONSTRAINT bot_state_revision_authority
    CHECK ((call_id IS NOT NULL AND migration_operation_id IS NULL) OR
           (call_id IS NULL AND migration_operation_id IS NOT NULL));
ALTER TABLE public.bot_state_revisions ADD CONSTRAINT bot_state_revision_migration
    FOREIGN KEY (bot_id,migration_operation_id) REFERENCES public.bot_state_migration_results(bot_id,operation_id);
