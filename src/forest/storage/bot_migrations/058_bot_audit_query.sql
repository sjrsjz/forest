-- Stable Bot audit metadata pagination and least-privilege read upgrades.
CREATE INDEX bot_audit_records_time_idx
    ON public.bot_audit_records(bot_id,created_at DESC,audit_id DESC);
CREATE INDEX bot_audit_records_action_time_idx
    ON public.bot_audit_records(bot_id,action,created_at DESC,audit_id DESC);

UPDATE public.bot_web_issuers i SET actions=COALESCE((
    SELECT jsonb_agg(action ORDER BY action) FROM (
        SELECT jsonb_array_elements_text(i.actions) AS action
        UNION
        SELECT 'audit.read' WHERE i.actions ?| ARRAY['config.read','message.read']
    ) permitted
), '[]'::jsonb);
