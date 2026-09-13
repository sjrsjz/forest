-- Retire the mistaken product-level Child Agent and add durable loop counters.
ALTER TABLE public.bot_agent_runs
 ADD COLUMN tool_iteration_count integer NOT NULL DEFAULT 0 CHECK(tool_iteration_count>=0),
 ADD COLUMN thinking_iteration_count integer NOT NULL DEFAULT 0 CHECK(thinking_iteration_count>=0),
 ADD COLUMN protocol_repair_count integer NOT NULL DEFAULT 0 CHECK(protocol_repair_count>=0);

DROP TRIGGER bot_agent_run_identity ON public.bot_agent_runs;
CREATE TRIGGER bot_agent_run_identity BEFORE UPDATE OR DELETE ON public.bot_agent_runs
 FOR EACH ROW EXECUTE FUNCTION public.forest_agent_fixed_fields(
 '{owner_epoch,actor_epoch,actor_host_incarnation,checkpoint_seq,next_step_seq,published_step_seq,current_step_id,phase,input_revision,selected_input_end_seq,wait_classified_seq,final_step_id,final_input_revision,final_reply_id,error_code,tool_iteration_count,thinking_iteration_count,protocol_repair_count}');

-- A request known not to have left Forest releases its old reservation.  A
-- dispatched request remains UNKNOWN because retirement cannot prove whether
-- the provider executed or billed it.
WITH released AS (
 SELECT bot_id,budget_day,sum(reserved_tokens)::bigint AS tokens
 FROM public.bot_child_model_attempts
 WHERE status='PERMITTED' AND NOT settled GROUP BY bot_id,budget_day
)
UPDATE public.bot_agent_budget_days d
 SET reserved_tokens=d.reserved_tokens-r.tokens
FROM released r WHERE (d.bot_id,d.budget_day)=(r.bot_id,r.budget_day);

WITH released AS (
 SELECT c.bot_id,c.parent_run_id AS run_id,sum(a.reserved_tokens)::bigint AS tokens
 FROM public.bot_child_model_attempts a
 JOIN public.bot_agent_children c USING(bot_id,child_id)
 WHERE a.status='PERMITTED' AND NOT a.settled GROUP BY c.bot_id,c.parent_run_id
)
UPDATE public.bot_agent_tree_budgets b
 SET reserved_tokens=b.reserved_tokens-r.tokens
FROM released r WHERE (b.bot_id,b.run_id)=(r.bot_id,r.run_id);

UPDATE public.bot_child_model_attempts
 SET status='NOT_SENT',settled=true,charged_tokens=0
 WHERE status='PERMITTED' AND NOT settled;
UPDATE public.bot_child_model_attempts SET status='UNKNOWN' WHERE status='DISPATCHING';
UPDATE public.bot_agent_children
 SET status=CASE WHEN EXISTS(
   SELECT 1 FROM public.bot_child_model_attempts a
   WHERE a.bot_id=bot_agent_children.bot_id AND a.child_id=bot_agent_children.child_id
     AND a.status='UNKNOWN'
 ) THEN 'UNKNOWN' ELSE 'CANCELLED' END,
 owner_id=NULL,lease_until=NULL,error_code=COALESCE(error_code,'CHILD_FEATURE_RETIRED')
 WHERE status IN ('QUEUED','RUNNING','RETRY','RESULT_READY','UNKNOWN');
UPDATE public.bot_agent_tools t
 SET status=CASE WHEN c.status='UNKNOWN' THEN 'UNKNOWN' ELSE 'CANCELLED' END
 FROM public.bot_agent_children c
 WHERE (t.bot_id,t.run_id,t.tool_call_id)=(c.bot_id,c.parent_run_id,c.tool_call_id)
   AND t.status='WAIT_CHILD';
UPDATE public.bot_agent_runs r SET phase=CASE
 WHEN EXISTS(SELECT 1 FROM public.bot_agent_tools t WHERE t.bot_id=r.bot_id AND t.run_id=r.run_id AND t.status='UNKNOWN')
 THEN 'BLOCKED_UNKNOWN' ELSE 'WAIT_TOOL' END,
 error_code=CASE
 WHEN EXISTS(SELECT 1 FROM public.bot_agent_tools t WHERE t.bot_id=r.bot_id AND t.run_id=r.run_id AND t.status='UNKNOWN')
 THEN 'CHILD_RESULT_UNKNOWN' ELSE NULL END
WHERE EXISTS(
 SELECT 1 FROM public.bot_agent_children c JOIN public.bot_agent_tools t
   ON (t.bot_id,t.run_id,t.tool_call_id)=(c.bot_id,c.parent_run_id,c.tool_call_id)
 WHERE c.bot_id=r.bot_id AND c.parent_run_id=r.run_id
   AND t.logical_step_id=r.current_step_id AND t.status IN ('CANCELLED','UNKNOWN')
)
  AND r.phase IN ('WAIT_TOOL','WAIT_APPROVAL','BLOCKED_UNKNOWN');

DROP TRIGGER bot_child_identity ON public.bot_agent_children;
DROP TRIGGER bot_child_result_once ON public.bot_agent_children;
DROP TRIGGER bot_child_snapshot_digest ON public.bot_agent_children;
DROP TRIGGER bot_child_attempt_identity ON public.bot_child_model_attempts;
DROP TRIGGER bot_child_attempt_result_once ON public.bot_child_model_attempts;
DROP TRIGGER bot_child_wire_digest ON public.bot_child_model_attempts;

CREATE FUNCTION public.forest_product_child_retired() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 RAISE EXCEPTION 'Product-level Child Agent is retired' USING ERRCODE='55000';
END $$;
CREATE TRIGGER bot_agent_children_retired BEFORE INSERT OR UPDATE OR DELETE
 ON public.bot_agent_children FOR EACH ROW EXECUTE FUNCTION public.forest_product_child_retired();
CREATE TRIGGER bot_child_attempts_retired BEFORE INSERT OR UPDATE OR DELETE
 ON public.bot_child_model_attempts FOR EACH ROW EXECUTE FUNCTION public.forest_product_child_retired();
