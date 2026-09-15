-- Ordinary-message handler heads switch atomically with component routes.
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM public.bot_schema_writer_gate
        WHERE singleton AND minimum_generation >= 19
    ) THEN
        RAISE EXCEPTION 'WRITER_GATE_NOT_INSTALLED' USING ERRCODE='55000';
    END IF;
END;
$$;

CREATE TABLE public.bot_message_handler_clocks (
    bot_id text PRIMARY KEY REFERENCES public.forest_bot_identity(bot_id),
    revision bigint NOT NULL DEFAULT 0 CHECK(revision>=0),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp()
);

CREATE TABLE public.bot_message_handler_generations (
    bot_id text NOT NULL,
    generation_id text NOT NULL,
    component_id text NOT NULL,
    package_digest text NOT NULL,
    spec_set_digest text NOT NULL,
    handler_count integer NOT NULL CHECK(handler_count BETWEEN 0 AND 64),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY(bot_id,generation_id),
    FOREIGN KEY(bot_id,generation_id)
        REFERENCES public.bot_generations(bot_id,generation_id)
);

-- Manifest schemas before 080 did not publish handler sets into Bot storage.
INSERT INTO public.bot_message_handler_generations(
    bot_id,generation_id,component_id,package_digest,spec_set_digest,handler_count
)
SELECT bot_id,generation_id,component_id,package_digest,
       'sha256:4f53cda18c2baa0c0354bb5f9a3ecbe5ed12ab4d8e11ba873c2f11161202b945',0
FROM public.bot_generations;

CREATE TABLE public.bot_generation_message_handler_definitions (
    bot_id text NOT NULL,
    generation_id text NOT NULL,
    component_id text NOT NULL,
    handler_id text NOT NULL,
    handler text NOT NULL,
    priority integer NOT NULL CHECK(priority BETWEEN -100 AND 100),
    spec_digest text NOT NULL,
    PRIMARY KEY(bot_id,generation_id,handler_id),
    FOREIGN KEY(bot_id,generation_id)
        REFERENCES public.bot_message_handler_generations(bot_id,generation_id)
);

CREATE TABLE public.bot_message_handler_publications (
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
        REFERENCES public.bot_message_handler_generations(bot_id,generation_id)
);

CREATE TABLE public.bot_message_handler_heads (
    bot_id text NOT NULL REFERENCES public.forest_bot_identity(bot_id),
    component_id text NOT NULL,
    handler_id text NOT NULL,
    generation_id text,
    route_epoch bigint NOT NULL CHECK(route_epoch>=1),
    registry_revision bigint NOT NULL CHECK(registry_revision>=1),
    handler text NOT NULL,
    priority integer NOT NULL CHECK(priority BETWEEN -100 AND 100),
    spec_digest text NOT NULL,
    active boolean NOT NULL,
    retired boolean NOT NULL DEFAULT false,
    management_revision bigint NOT NULL DEFAULT 0 CHECK(management_revision>=0),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    retired_at timestamptz,
    PRIMARY KEY(bot_id,component_id,handler_id),
    FOREIGN KEY(bot_id,generation_id)
        REFERENCES public.bot_message_handler_generations(bot_id,generation_id),
    CHECK(NOT retired OR (NOT active AND retired_at IS NOT NULL)),
    CHECK((active AND generation_id IS NOT NULL) OR (NOT active AND generation_id IS NULL))
);

CREATE TABLE public.bot_message_handler_retirements (
    bot_id text NOT NULL REFERENCES public.forest_bot_identity(bot_id),
    operation_id text NOT NULL,
    component_id text NOT NULL,
    handler_id text NOT NULL,
    expected_management_revision bigint NOT NULL CHECK(expected_management_revision>=0),
    resulting_management_revision bigint NOT NULL CHECK(resulting_management_revision>=1),
    registry_revision bigint NOT NULL CHECK(registry_revision>=1),
    host_incarnation text NOT NULL,
    host_owner_epoch bigint NOT NULL,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY(bot_id,operation_id),
    UNIQUE(bot_id,component_id,handler_id),
    FOREIGN KEY(bot_id,component_id,handler_id)
        REFERENCES public.bot_message_handler_heads(bot_id,component_id,handler_id)
);

CREATE FUNCTION public.forest_message_handler_fact_immutable_guard() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    RAISE EXCEPTION 'MESSAGE_HANDLER_REGISTRY_FACT_IMMUTABLE' USING ERRCODE='55000';
END;
$$;
CREATE TRIGGER bot_message_handler_generation_immutable
    BEFORE UPDATE OR DELETE ON public.bot_message_handler_generations
    FOR EACH ROW EXECUTE FUNCTION public.forest_message_handler_fact_immutable_guard();
CREATE TRIGGER bot_generation_message_handler_definition_immutable
    BEFORE UPDATE OR DELETE ON public.bot_generation_message_handler_definitions
    FOR EACH ROW EXECUTE FUNCTION public.forest_message_handler_fact_immutable_guard();
CREATE TRIGGER bot_message_handler_publication_immutable
    BEFORE UPDATE OR DELETE ON public.bot_message_handler_publications
    FOR EACH ROW EXECUTE FUNCTION public.forest_message_handler_fact_immutable_guard();
CREATE TRIGGER bot_message_handler_retirement_immutable
    BEFORE UPDATE OR DELETE ON public.bot_message_handler_retirements
    FOR EACH ROW EXECUTE FUNCTION public.forest_message_handler_fact_immutable_guard();

CREATE FUNCTION public.forest_message_handler_head_guard() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    IF TG_OP='DELETE' THEN
        RAISE EXCEPTION 'MESSAGE_HANDLER_HEAD_DELETE_FORBIDDEN' USING ERRCODE='55000';
    END IF;
    IF ROW(NEW.bot_id,NEW.component_id,NEW.handler_id)
       IS DISTINCT FROM ROW(OLD.bot_id,OLD.component_id,OLD.handler_id) THEN
        RAISE EXCEPTION 'MESSAGE_HANDLER_IDENTITY_IMMUTABLE' USING ERRCODE='55000';
    END IF;
    IF OLD.retired AND NOT NEW.retired THEN
        RAISE EXCEPTION 'MESSAGE_HANDLER_RETIREMENT_IRREVERSIBLE' USING ERRCODE='55000';
    END IF;
    RETURN NEW;
END;
$$;
CREATE TRIGGER bot_message_handler_head_guard
    BEFORE UPDATE OR DELETE ON public.bot_message_handler_heads
    FOR EACH ROW EXECUTE FUNCTION public.forest_message_handler_head_guard();
