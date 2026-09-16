-- A Bot-wide source ledger with independent group trunks. Content is owned by
-- PayloadStore; eligibility is an immutable observation of a committed cut.
CREATE TABLE public.bot_memory_sources (
    bot_id text NOT NULL REFERENCES public.forest_bot_identity(bot_id),
    source_id text NOT NULL, source_kind text NOT NULL CHECK(source_kind IN ('GROUP','DIRECT','SYSTEM')),
    visibility text NOT NULL CHECK(visibility IN ('bot_shared','source_restricted')),
    platform text NOT NULL, realm_id text NOT NULL, identity_schema integer NOT NULL DEFAULT 1 CHECK(identity_schema=1),
    channel_kind text NOT NULL, channel_id text NOT NULL,
    target_group_source_id text NOT NULL, owner_conversation_id text,
    next_order_seq bigint NOT NULL DEFAULT 1 CHECK(next_order_seq>0),
    has_reconstructed_order boolean NOT NULL DEFAULT false,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY(bot_id,source_id),
    FOREIGN KEY(bot_id,owner_conversation_id) REFERENCES public.bot_conversations(bot_id,conversation_id)
);
CREATE TABLE public.bot_memory_source_units (
    bot_id text NOT NULL, source_unit_id text NOT NULL, source_id text NOT NULL,
    source_event_id text NOT NULL, source_revision bigint NOT NULL DEFAULT 1 CHECK(source_revision=1),
    source_order_seq bigint NOT NULL CHECK(source_order_seq>0),
    order_origin text NOT NULL CHECK(order_origin IN ('LIVE','RECONSTRUCTED')),
    kind text NOT NULL CHECK(kind IN ('INPUT','REPLY')),
    principal_ref text NOT NULL, identity_origin text NOT NULL,
    payload_id text NOT NULL, content_digest text NOT NULL,
    lineage_complete boolean NOT NULL DEFAULT true,
    occurred_at timestamptz NOT NULL, accepted_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY(bot_id,source_unit_id), UNIQUE(bot_id,source_event_id),
    UNIQUE(bot_id,source_id,source_order_seq),
    FOREIGN KEY(bot_id,source_id) REFERENCES public.bot_memory_sources(bot_id,source_id),
    FOREIGN KEY(bot_id,payload_id) REFERENCES public.bot_payload_headers(bot_id,payload_id)
);
CREATE TABLE public.bot_memory_source_occurrences (
    bot_id text NOT NULL, occurrence_id text NOT NULL, source_unit_id text NOT NULL,
    conversation_id text NOT NULL, binding_id text NOT NULL, thread_id text NOT NULL,
    origin text NOT NULL CHECK(origin IN ('INPUT','REPLY')),
    event_id text, reply_id text, ingress_seq bigint NOT NULL CHECK(ingress_seq>=0),
    accepted_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY(bot_id,occurrence_id), UNIQUE(bot_id,event_id), UNIQUE(bot_id,reply_id),
    FOREIGN KEY(bot_id,source_unit_id) REFERENCES public.bot_memory_source_units(bot_id,source_unit_id),
    FOREIGN KEY(bot_id,conversation_id) REFERENCES public.bot_conversations(bot_id,conversation_id),
    FOREIGN KEY(bot_id,event_id) REFERENCES public.bot_message_inbox(bot_id,event_id),
    FOREIGN KEY(bot_id,reply_id) REFERENCES public.bot_conversation_replies(bot_id,reply_id),
    CHECK((origin='INPUT' AND event_id IS NOT NULL AND reply_id IS NULL)
       OR (origin='REPLY' AND reply_id IS NOT NULL AND event_id IS NULL))
);
CREATE TABLE public.bot_memory_source_eligibility (
    bot_id text NOT NULL, occurrence_id text NOT NULL, input_batch_id text,
    context_revision bigint NOT NULL CHECK(context_revision>=0), eligible_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY(bot_id,occurrence_id),
    FOREIGN KEY(bot_id,occurrence_id) REFERENCES public.bot_memory_source_occurrences(bot_id,occurrence_id),
    FOREIGN KEY(bot_id,input_batch_id) REFERENCES public.bot_context_input_batches(bot_id,input_batch_id)
);
CREATE TABLE public.bot_memory_source_media (
    bot_id text NOT NULL, source_unit_id text NOT NULL, part_id text NOT NULL,
    occurrence_id text NOT NULL, media_ref text NOT NULL,
    PRIMARY KEY(bot_id,source_unit_id,part_id,occurrence_id),
    FOREIGN KEY(bot_id,source_unit_id) REFERENCES public.bot_memory_source_units(bot_id,source_unit_id),
    FOREIGN KEY(bot_id,occurrence_id) REFERENCES public.bot_memory_source_occurrences(bot_id,occurrence_id),
    FOREIGN KEY(bot_id,media_ref) REFERENCES public.bot_inbound_media_refs(bot_id,media_ref)
);
CREATE INDEX bot_memory_source_observation_cut ON public.bot_memory_source_occurrences(bot_id,conversation_id,ingress_seq,source_unit_id);
CREATE INDEX bot_memory_source_observation_unit ON public.bot_memory_source_occurrences(bot_id,source_unit_id);
CREATE INDEX bot_memory_sources_trunk
    ON public.bot_memory_sources(bot_id,target_group_source_id,source_id);
CREATE INDEX bot_memory_source_units_browse
    ON public.bot_memory_source_units(bot_id,occurred_at DESC,source_unit_id DESC);
CREATE INDEX bot_memory_source_units_source_browse
    ON public.bot_memory_source_units(bot_id,source_id,occurred_at DESC,source_unit_id DESC);
CREATE INDEX bot_memory_source_units_principal_browse
    ON public.bot_memory_source_units(bot_id,principal_ref,occurred_at DESC,source_unit_id DESC);
CREATE INDEX bot_memory_source_units_kind_browse
    ON public.bot_memory_source_units(bot_id,kind,occurred_at DESC,source_unit_id DESC);
CREATE TABLE public.bot_memory_backfill_heads (
    bot_id text NOT NULL REFERENCES public.forest_bot_identity(bot_id),
    kind text NOT NULL CHECK(kind IN ('INPUT','REPLY')), cursor_id text NOT NULL DEFAULT '',
    completed boolean NOT NULL DEFAULT false,
    PRIMARY KEY(bot_id,kind)
);
CREATE TRIGGER bot_memory_source_identity BEFORE UPDATE OR DELETE ON public.bot_memory_sources
    FOR EACH ROW EXECUTE FUNCTION public.forest_agent_fixed_fields('{next_order_seq,has_reconstructed_order}');
CREATE TRIGGER bot_memory_source_units_immutable BEFORE UPDATE OR DELETE ON public.bot_memory_source_units
    FOR EACH ROW EXECUTE FUNCTION public.forest_bot_reject_mutation();
CREATE TRIGGER bot_memory_source_occurrences_immutable BEFORE UPDATE OR DELETE ON public.bot_memory_source_occurrences
    FOR EACH ROW EXECUTE FUNCTION public.forest_bot_reject_mutation();
CREATE TRIGGER bot_memory_source_eligibility_immutable BEFORE UPDATE OR DELETE ON public.bot_memory_source_eligibility
    FOR EACH ROW EXECUTE FUNCTION public.forest_bot_reject_mutation();
CREATE TRIGGER bot_memory_source_media_immutable BEFORE UPDATE OR DELETE ON public.bot_memory_source_media
    FOR EACH ROW EXECUTE FUNCTION public.forest_bot_reject_mutation();
