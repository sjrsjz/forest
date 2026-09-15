-- Purpose-bound Web diagnostics and bounded recovery for command work.
DO $$
BEGIN
 IF NOT EXISTS(SELECT 1 FROM public.bot_schema_writer_gate
               WHERE singleton AND minimum_generation>=17) THEN
  RAISE EXCEPTION 'WRITER_GATE_NOT_INSTALLED' USING ERRCODE='55000';
 END IF;
END;
$$;

UPDATE public.bot_web_issuers i SET actions=(
 SELECT jsonb_agg(action ORDER BY action) FROM (
  SELECT DISTINCT action FROM jsonb_array_elements_text(
   i.actions ||
   CASE WHEN i.actions ?| ARRAY['message.read','command.policy.read']
    THEN '["command.execution.read"]'::jsonb ELSE '[]'::jsonb END ||
   CASE WHEN i.actions ?| ARRAY['message.cancel','command.policy.apply']
    THEN '["command.execution.recover"]'::jsonb ELSE '[]'::jsonb END
  ) AS action
 ) AS expanded
);
