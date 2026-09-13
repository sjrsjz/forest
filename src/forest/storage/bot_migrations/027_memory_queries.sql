-- Query identity and chosen evidence are permanent; plaintext stays removable.
CREATE TABLE public.bot_memory_queries (
 bot_id text NOT NULL, query_id text NOT NULL, input_digest text NOT NULL,
 config_revision bigint NOT NULL, conversation_id text NOT NULL, input_cut bigint NOT NULL CHECK(input_cut>=0),
 space_id text NOT NULL, source_unit_ids jsonb NOT NULL, facet_ids jsonb NOT NULL,
 query_modalities jsonb NOT NULL, deletion_epoch bigint NOT NULL, runtime_fence jsonb NOT NULL,
 created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
 PRIMARY KEY(bot_id,query_id),
 FOREIGN KEY(bot_id,config_revision) REFERENCES public.bot_memory_config_revisions(bot_id,revision),
 FOREIGN KEY(bot_id,conversation_id) REFERENCES public.bot_conversations(bot_id,conversation_id),
 CHECK(jsonb_array_length(source_unit_ids) BETWEEN 1 AND 32),
 CHECK(jsonb_array_length(facet_ids) BETWEEN 1 AND 32)
);
CREATE TABLE public.bot_memory_evidence_bundles (
 bot_id text NOT NULL, query_id text NOT NULL, bundle_id text NOT NULL,
 payload_id text, digest text NOT NULL, status text NOT NULL CHECK(status IN ('READY','DEGRADED')),
 reason_code text, deletion_epoch bigint NOT NULL, fact_revision bigint NOT NULL,
 source_unit_ids jsonb NOT NULL, completeness jsonb NOT NULL,
 created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
 PRIMARY KEY(bot_id,query_id), UNIQUE(bot_id,bundle_id),
 FOREIGN KEY(bot_id,query_id) REFERENCES public.bot_memory_queries(bot_id,query_id),
 FOREIGN KEY(bot_id,payload_id) REFERENCES public.bot_payload_headers(bot_id,payload_id),
 CHECK(payload_id IS NOT NULL OR (status='DEGRADED' AND jsonb_array_length(source_unit_ids)=0))
);
CREATE TRIGGER memory_query_immutable BEFORE UPDATE OR DELETE ON public.bot_memory_queries
 FOR EACH ROW EXECUTE FUNCTION public.forest_bot_reject_mutation();
CREATE TRIGGER memory_evidence_bundle_immutable BEFORE UPDATE OR DELETE ON public.bot_memory_evidence_bundles
 FOR EACH ROW EXECUTE FUNCTION public.forest_bot_reject_mutation();
