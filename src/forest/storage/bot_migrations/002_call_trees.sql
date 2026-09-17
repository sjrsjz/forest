ALTER TABLE public.bot_calls ADD COLUMN parent_call_id text;
ALTER TABLE public.bot_calls ADD COLUMN root_call_id text;
ALTER TABLE public.bot_calls ADD COLUMN call_depth integer NOT NULL DEFAULT 0 CHECK (call_depth BETWEEN 0 AND 8);
ALTER TABLE public.bot_calls ADD COLUMN dependency_components text[] NOT NULL DEFAULT '{}';
UPDATE public.bot_calls SET root_call_id=call_id;
ALTER TABLE public.bot_calls ALTER COLUMN root_call_id SET NOT NULL;
ALTER TABLE public.bot_calls ADD CONSTRAINT bot_call_parent_fk
    FOREIGN KEY (bot_id,parent_call_id) REFERENCES public.bot_calls(bot_id,call_id);
ALTER TABLE public.bot_calls ADD CONSTRAINT bot_call_root_fk
    FOREIGN KEY (bot_id,root_call_id) REFERENCES public.bot_calls(bot_id,call_id);
ALTER TABLE public.bot_calls ADD CONSTRAINT bot_call_root_shape CHECK (
    (parent_call_id IS NULL AND root_call_id=call_id AND call_depth=0)
    OR (parent_call_id IS NOT NULL AND root_call_id<>call_id AND call_depth>0)
);
CREATE INDEX bot_calls_parent_idx ON public.bot_calls(bot_id,parent_call_id);
CREATE INDEX bot_calls_root_idx ON public.bot_calls(bot_id,root_call_id,call_depth,call_id);
CREATE INDEX bot_calls_causal_root_idx ON public.bot_calls
    (bot_id,causal_root_kind,causal_root_id,component_id,generation_id,status);

ALTER TABLE public.bot_generations DROP CONSTRAINT bot_generations_status_check;
ALTER TABLE public.bot_generations ADD COLUMN capability_grants jsonb NOT NULL DEFAULT '[]'::jsonb;
CREATE TRIGGER bot_generation_grants_immutable BEFORE UPDATE OF capability_grants ON public.bot_generations
    FOR EACH ROW WHEN (OLD.capability_grants IS DISTINCT FROM NEW.capability_grants)
    EXECUTE FUNCTION public.forest_bot_reject_mutation();
ALTER TABLE public.bot_generations ADD CONSTRAINT bot_generations_status_check
    CHECK (status IN ('PREPARING','READY','ACTIVE','QUIESCING','QUIESCENT','RETIRED','FAILED','DISPOSED'));
ALTER TABLE public.bot_lifecycle_operations DROP CONSTRAINT bot_lifecycle_operations_phase_check;
ALTER TABLE public.bot_lifecycle_operations ADD CONSTRAINT bot_lifecycle_operations_phase_check
    CHECK (phase IN ('PLAN','FROZEN','QUIESCENT','COMMITTED','FAILED','ABANDONED'));

CREATE FUNCTION public.forest_call_identity() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF ROW(OLD.bot_id, OLD.call_id, OLD.operation_id, OLD.component_id, OLD.generation_id,
        OLD.route_epoch, OLD.worker_incarnation, OLD.host_id, OLD.host_incarnation, OLD.owner_epoch,
        OLD.storage_generation, OLD.maintenance_epoch, OLD.policy_revision, OLD.deadline_at,
        OLD.input_digest, OLD.causal_root_kind, OLD.causal_root_id,
        OLD.parent_call_id, OLD.root_call_id, OLD.call_depth, OLD.dependency_components)
      IS DISTINCT FROM ROW(NEW.bot_id, NEW.call_id, NEW.operation_id, NEW.component_id, NEW.generation_id,
        NEW.route_epoch, NEW.worker_incarnation, NEW.host_id, NEW.host_incarnation, NEW.owner_epoch,
        NEW.storage_generation, NEW.maintenance_epoch, NEW.policy_revision, NEW.deadline_at,
        NEW.input_digest, NEW.causal_root_kind, NEW.causal_root_id,
        NEW.parent_call_id, NEW.root_call_id, NEW.call_depth, NEW.dependency_components) THEN
        RAISE EXCEPTION 'Forest call identity is immutable' USING ERRCODE = '55000';
    END IF;
    RETURN NEW;
END;
$$;
CREATE TRIGGER bot_call_identity BEFORE UPDATE ON public.bot_calls
    FOR EACH ROW EXECUTE FUNCTION public.forest_call_identity();

-- Eligibility includes every ancestor, so an orphaned nested call is never counted as
-- a live executor merely because its own target route and deadline remain live.
CREATE VIEW public.bot_live_calls AS
WITH RECURSIVE eligible AS (
    SELECT c.* FROM public.bot_calls c
    JOIN public.bot_runtime_state b ON b.bot_id=c.bot_id
    JOIN public.bot_host_lease h ON h.bot_id=c.bot_id
    JOIN public.bot_component_routes r ON r.bot_id=c.bot_id AND r.component_id=c.component_id
    JOIN public.bot_generations g ON g.bot_id=c.bot_id AND g.generation_id=c.generation_id
    WHERE c.status='ADMITTED' AND c.deadline_at>clock_timestamp()
      AND b.status='ACTIVE' AND c.storage_generation=b.storage_generation
      AND c.maintenance_epoch=b.maintenance_epoch AND c.policy_revision=b.policy_revision
      AND c.owner_epoch=h.owner_epoch AND c.host_incarnation=h.incarnation AND c.host_id=h.host_id
      AND h.expires_at>clock_timestamp() AND c.route_epoch=r.route_epoch
      AND c.generation_id=r.generation_id AND g.status IN ('ACTIVE','QUIESCING')
      AND c.worker_incarnation=g.worker_incarnation
), live AS (
    SELECT e.* FROM eligible e WHERE e.parent_call_id IS NULL AND e.call_depth=0 AND e.root_call_id=e.call_id
    UNION ALL
    SELECT e.* FROM eligible e JOIN live p ON p.bot_id=e.bot_id AND p.call_id=e.parent_call_id
    WHERE e.root_call_id=p.root_call_id AND e.call_depth=p.call_depth+1
      AND e.deadline_at<=p.deadline_at AND e.call_depth<=8
)
SELECT * FROM live;
