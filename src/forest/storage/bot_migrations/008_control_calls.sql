-- The host's pure OneBot codec has an explicit bounded CONTROL lane. Normal
-- Worker service calls never select this lane. Reload still closes both lanes.
ALTER TABLE public.bot_component_routes ADD COLUMN control_admission boolean NOT NULL DEFAULT false;
ALTER TABLE public.bot_component_routes ADD CONSTRAINT bot_control_route_scope
    CHECK (NOT control_admission OR component_id='adapter.onebot');
ALTER TABLE public.bot_calls ADD COLUMN admission_lane text NOT NULL DEFAULT 'DATA'
    CHECK (admission_lane IN ('DATA','CONTROL'));
ALTER TABLE public.bot_calls ADD COLUMN control_service text;
ALTER TABLE public.bot_calls ADD CONSTRAINT bot_control_call_shape CHECK (
    (admission_lane='DATA' AND control_service IS NULL)
    OR (admission_lane='CONTROL' AND component_id='adapter.onebot'
        AND control_service IN ('onebot.normalize','onebot.prepare')
        AND control_service IS NOT NULL AND parent_call_id IS NULL
        AND dependency_components='{}'::text[])
);
CREATE TRIGGER bot_control_call_immutable BEFORE UPDATE OF admission_lane,control_service ON public.bot_calls
    FOR EACH ROW WHEN (ROW(OLD.admission_lane,OLD.control_service)
        IS DISTINCT FROM ROW(NEW.admission_lane,NEW.control_service))
    EXECUTE FUNCTION public.forest_bot_reject_mutation();

CREATE OR REPLACE VIEW public.bot_live_calls AS
WITH RECURSIVE eligible AS (
    SELECT c.* FROM public.bot_calls c
    JOIN public.bot_runtime_state b ON b.bot_id=c.bot_id
    JOIN public.bot_host_lease h ON h.bot_id=c.bot_id
    JOIN public.bot_component_routes r ON r.bot_id=c.bot_id AND r.component_id=c.component_id
    JOIN public.bot_generations g ON g.bot_id=c.bot_id AND g.generation_id=c.generation_id
    WHERE c.status='ADMITTED' AND c.deadline_at>clock_timestamp()
      AND b.status='ACTIVE' AND c.storage_generation=b.storage_generation
      AND ((c.admission_lane='DATA' AND c.maintenance_epoch=b.maintenance_epoch)
           OR (c.admission_lane='CONTROL' AND b.control_admission))
      AND c.policy_revision=b.policy_revision
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
      AND e.admission_lane='DATA' AND p.admission_lane='DATA'
)
SELECT * FROM live;
