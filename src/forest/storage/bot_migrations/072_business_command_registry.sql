-- Command heads must change in the same transaction as component routes.
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM public.bot_schema_writer_gate
        WHERE singleton AND minimum_generation >= 12
    ) THEN
        RAISE EXCEPTION 'WRITER_GATE_NOT_INSTALLED' USING ERRCODE='55000';
    END IF;
END;
$$;

CREATE TABLE public.bot_command_clocks (
    bot_id text PRIMARY KEY REFERENCES public.forest_bot_identity(bot_id),
    revision bigint NOT NULL DEFAULT 0 CHECK(revision>=0),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp()
);

CREATE TABLE public.bot_command_generations (
    bot_id text NOT NULL,
    generation_id text NOT NULL,
    component_id text NOT NULL,
    package_digest text NOT NULL,
    spec_set_digest text NOT NULL,
    command_count integer NOT NULL CHECK(command_count BETWEEN 0 AND 64),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY(bot_id,generation_id),
    FOREIGN KEY(bot_id,generation_id)
        REFERENCES public.bot_generations(bot_id,generation_id)
);

-- Every pre-072 generation was admitted by a manifest schema that rejected
-- business commands, so its only truthful declaration set is the empty set.
INSERT INTO public.bot_command_generations(
    bot_id,generation_id,component_id,package_digest,spec_set_digest,command_count
)
SELECT bot_id,generation_id,component_id,package_digest,
       'sha256:4f53cda18c2baa0c0354bb5f9a3ecbe5ed12ab4d8e11ba873c2f11161202b945',0
FROM public.bot_generations;

CREATE TABLE public.bot_generation_command_definitions (
    bot_id text NOT NULL,
    generation_id text NOT NULL,
    component_id text NOT NULL,
    command_id text NOT NULL,
    name text NOT NULL,
    handler text NOT NULL,
    permission text NOT NULL,
    argument_mode text NOT NULL CHECK(argument_mode IN ('none','tail_text')),
    input_schema text NOT NULL,
    schema_digest text NOT NULL,
    priority integer NOT NULL CHECK(priority BETWEEN -100 AND 100),
    spec_digest text NOT NULL,
    PRIMARY KEY(bot_id,generation_id,command_id),
    UNIQUE(bot_id,generation_id,name),
    FOREIGN KEY(bot_id,generation_id)
        REFERENCES public.bot_command_generations(bot_id,generation_id)
);

CREATE TABLE public.bot_command_publications (
    bot_id text NOT NULL REFERENCES public.forest_bot_identity(bot_id),
    registry_revision bigint NOT NULL CHECK(registry_revision>=1),
    component_id text NOT NULL,
    generation_id text,
    route_epoch bigint NOT NULL CHECK(route_epoch>=1),
    operation_kind text NOT NULL CHECK(operation_kind IN ('INSTALL','DISABLE')),
    spec_set_digest text NOT NULL,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY(bot_id,registry_revision),
    UNIQUE(bot_id,component_id,route_epoch),
    FOREIGN KEY(bot_id,generation_id)
        REFERENCES public.bot_command_generations(bot_id,generation_id)
);

CREATE TABLE public.bot_command_heads (
    bot_id text NOT NULL REFERENCES public.forest_bot_identity(bot_id),
    command_id text NOT NULL,
    component_id text NOT NULL,
    generation_id text,
    route_epoch bigint NOT NULL CHECK(route_epoch>=1),
    registry_revision bigint NOT NULL CHECK(registry_revision>=1),
    name text NOT NULL,
    handler text NOT NULL,
    permission text NOT NULL,
    argument_mode text NOT NULL CHECK(argument_mode IN ('none','tail_text')),
    input_schema text NOT NULL,
    schema_digest text NOT NULL,
    priority integer NOT NULL CHECK(priority BETWEEN -100 AND 100),
    spec_digest text NOT NULL,
    active boolean NOT NULL,
    retired boolean NOT NULL DEFAULT false,
    management_revision bigint NOT NULL DEFAULT 0 CHECK(management_revision>=0),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    retired_at timestamptz,
    PRIMARY KEY(bot_id,command_id),
    FOREIGN KEY(bot_id,generation_id)
        REFERENCES public.bot_command_generations(bot_id,generation_id),
    CHECK(NOT retired OR (NOT active AND retired_at IS NOT NULL)),
    CHECK((active AND generation_id IS NOT NULL) OR (NOT active AND generation_id IS NULL))
);
CREATE UNIQUE INDEX bot_command_active_name_unique
    ON public.bot_command_heads(bot_id,name) WHERE active;

CREATE TABLE public.bot_command_retirements (
    bot_id text NOT NULL REFERENCES public.forest_bot_identity(bot_id),
    operation_id text NOT NULL,
    component_id text NOT NULL,
    command_id text NOT NULL,
    expected_management_revision bigint NOT NULL CHECK(expected_management_revision>=0),
    resulting_management_revision bigint NOT NULL CHECK(resulting_management_revision>=1),
    registry_revision bigint NOT NULL CHECK(registry_revision>=1),
    host_incarnation text NOT NULL,
    host_owner_epoch bigint NOT NULL,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY(bot_id,operation_id),
    UNIQUE(bot_id,command_id),
    FOREIGN KEY(bot_id,command_id)
        REFERENCES public.bot_command_heads(bot_id,command_id)
);

CREATE FUNCTION public.forest_command_immutable_guard() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    RAISE EXCEPTION 'COMMAND_REGISTRY_FACT_IMMUTABLE' USING ERRCODE='55000';
END;
$$;
CREATE TRIGGER bot_command_generation_immutable
    BEFORE UPDATE OR DELETE ON public.bot_command_generations
    FOR EACH ROW EXECUTE FUNCTION public.forest_command_immutable_guard();
CREATE TRIGGER bot_generation_command_definition_immutable
    BEFORE UPDATE OR DELETE ON public.bot_generation_command_definitions
    FOR EACH ROW EXECUTE FUNCTION public.forest_command_immutable_guard();
CREATE TRIGGER bot_command_publication_immutable
    BEFORE UPDATE OR DELETE ON public.bot_command_publications
    FOR EACH ROW EXECUTE FUNCTION public.forest_command_immutable_guard();
CREATE TRIGGER bot_command_retirement_immutable
    BEFORE UPDATE OR DELETE ON public.bot_command_retirements
    FOR EACH ROW EXECUTE FUNCTION public.forest_command_immutable_guard();

CREATE FUNCTION public.forest_command_head_guard() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    IF TG_OP='DELETE' THEN
        RAISE EXCEPTION 'COMMAND_HEAD_DELETE_FORBIDDEN' USING ERRCODE='55000';
    END IF;
    IF ROW(NEW.bot_id,NEW.command_id,NEW.component_id,NEW.name)
       IS DISTINCT FROM ROW(OLD.bot_id,OLD.command_id,OLD.component_id,OLD.name) THEN
        RAISE EXCEPTION 'COMMAND_IDENTITY_IMMUTABLE' USING ERRCODE='55000';
    END IF;
    IF OLD.retired AND NOT NEW.retired THEN
        RAISE EXCEPTION 'COMMAND_RETIREMENT_IRREVERSIBLE' USING ERRCODE='55000';
    END IF;
    RETURN NEW;
END;
$$;
CREATE TRIGGER bot_command_head_guard
    BEFORE UPDATE OR DELETE ON public.bot_command_heads
    FOR EACH ROW EXECUTE FUNCTION public.forest_command_head_guard();
