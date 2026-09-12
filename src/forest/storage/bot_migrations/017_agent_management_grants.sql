-- Upgrade the existing first-party management authority without replacing keys,
-- enabling disabled issuers, or granting write powers to a restricted issuer.
-- Each new domain action requires its corresponding already-granted operation.
UPDATE public.bot_web_issuers i SET actions=COALESCE((
    SELECT jsonb_agg(action ORDER BY action) FROM (
        SELECT jsonb_array_elements_text(i.actions) AS action
        UNION
        SELECT capability.action FROM (VALUES
            ('config.history', '["config.read"]'::jsonb),
            ('agent.config.read', '["config.read"]'::jsonb),
            ('agent.config.plan', '["config.plan"]'::jsonb),
            ('agent.config.apply', '["config.apply"]'::jsonb),
            ('agent.read', '["message.read"]'::jsonb),
            ('agent.cancel', '["message.cancel"]'::jsonb),
            ('agent.approval', '["config.apply","service.invoke"]'::jsonb),
            ('agent.resolve', '["delivery.reconcile","service.invoke"]'::jsonb)
        ) AS capability(action, required)
        WHERE i.actions @> capability.required
    ) permitted
), '[]'::jsonb);
