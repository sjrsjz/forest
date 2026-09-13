-- A model step chooses its retrieval exactly once. Content remains in PayloadStore.
CREATE TABLE public.bot_agent_memory_bindings (
 bot_id text NOT NULL, logical_step_id text NOT NULL, run_id text NOT NULL,
 snapshot_digest text NOT NULL, input_cut bigint NOT NULL, input_revision bigint NOT NULL,
 config_revision bigint NOT NULL, query_id text, source_unit_ids jsonb NOT NULL,
 status text NOT NULL CHECK(status IN ('DISABLED','PENDING','BOUND','BYPASSED')),
 bundle_id text, bundle_digest text, error_code text,
 created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
 PRIMARY KEY(bot_id,logical_step_id),
 FOREIGN KEY(bot_id,logical_step_id) REFERENCES public.bot_agent_steps(bot_id,logical_step_id),
 FOREIGN KEY(bot_id,run_id) REFERENCES public.bot_agent_runs(bot_id,run_id),
 FOREIGN KEY(bot_id,bundle_id) REFERENCES public.bot_memory_evidence_bundles(bot_id,bundle_id),
 CHECK(input_cut>=0 AND input_revision>=0 AND config_revision>=0),
 CHECK(jsonb_typeof(source_unit_ids)='array' AND jsonb_array_length(source_unit_ids)<=32),
 CHECK((status='BOUND')=(bundle_id IS NOT NULL AND bundle_digest IS NOT NULL)),
 CHECK(status='DISABLED' OR query_id IS NOT NULL)
);
CREATE INDEX agent_memory_run ON public.bot_agent_memory_bindings(bot_id,run_id);
CREATE FUNCTION public.forest_agent_memory_transition() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 IF TG_OP='DELETE' THEN RAISE EXCEPTION 'Agent memory binding is permanent'; END IF;
 IF OLD.status<>'PENDING' OR NEW.status NOT IN ('BOUND','BYPASSED') OR
    (to_jsonb(OLD)-ARRAY['status','bundle_id','bundle_digest','error_code']) IS DISTINCT FROM
    (to_jsonb(NEW)-ARRAY['status','bundle_id','bundle_digest','error_code']) THEN
   RAISE EXCEPTION 'Agent memory decision is already frozen';
 END IF;
 RETURN NEW;
END $$;
CREATE TRIGGER agent_memory_transition BEFORE UPDATE OR DELETE ON public.bot_agent_memory_bindings
 FOR EACH ROW EXECUTE FUNCTION public.forest_agent_memory_transition();
