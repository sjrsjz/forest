-- Bot-wide maintenance is more powerful than a conversation cancellation.
-- Existing issuers gain read from message.read and write only when they already
-- hold both configuration-apply and message-cancel capabilities.
UPDATE public.bot_web_issuers i SET actions=COALESCE((
    SELECT jsonb_agg(action ORDER BY action) FROM (
        SELECT jsonb_array_elements_text(i.actions) AS action
        UNION
        SELECT capability.action FROM (VALUES
            ('bot.runtime.read', '["message.read"]'::jsonb),
            ('bot.maintenance', '["config.apply","message.cancel"]'::jsonb)
        ) AS capability(action,required)
        WHERE i.actions @> capability.required
    ) permitted
), '[]'::jsonb);
