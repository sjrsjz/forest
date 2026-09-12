ALTER TABLE public.bot_config_activations
    ADD COLUMN recover_effective boolean NOT NULL DEFAULT false;

-- Restart recovery may recreate only the CURRENT effective revision. It cannot
-- mark an old desired revision effective or copy an old durable state snapshot.
CREATE OR REPLACE FUNCTION public.forest_publish_configuration() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE activation record; head record; cfg_revision record; candidate record;
BEGIN
    IF NEW.phase <> 'COMMITTED' OR OLD.phase = 'COMMITTED' THEN RETURN NEW; END IF;
    SELECT * INTO activation FROM public.bot_config_activations
        WHERE bot_id=NEW.bot_id AND operation_id=NEW.operation_id;
    IF NOT FOUND THEN RETURN NEW; END IF;
    SELECT * INTO head FROM public.bot_config_heads
        WHERE bot_id=NEW.bot_id AND component_id=activation.component_id FOR UPDATE;
    SELECT * INTO cfg_revision FROM public.bot_config_revisions
        WHERE bot_id=NEW.bot_id AND component_id=activation.component_id AND revision=activation.revision;
    SELECT * INTO candidate FROM public.bot_generations
        WHERE bot_id=NEW.bot_id AND generation_id=NEW.candidate_generation_id;
    IF (CASE WHEN activation.recover_effective THEN head.effective_revision ELSE head.desired_revision END)
            IS DISTINCT FROM activation.revision
        OR activation.component_id IS DISTINCT FROM NEW.component_id
        OR activation.host_incarnation IS DISTINCT FROM NEW.host_incarnation
        OR candidate.package_digest IS DISTINCT FROM (cfg_revision.values->>'package_digest')
        OR candidate.config IS DISTINCT FROM (cfg_revision.values->'config')
        OR candidate.capability_grants IS DISTINCT FROM (cfg_revision.values->'grants')
        OR cfg_revision.policy_revision IS DISTINCT FROM (SELECT policy_revision FROM public.bot_runtime_state
                                                        WHERE bot_id=NEW.bot_id) THEN
        RAISE EXCEPTION 'CONFIGURATION_SUPERSEDED' USING ERRCODE='P0001';
    END IF;
    IF NOT activation.recover_effective THEN
        UPDATE public.bot_config_heads SET effective_revision=activation.revision
            WHERE bot_id=NEW.bot_id AND component_id=activation.component_id;
    END IF;
    INSERT INTO public.bot_outbox_events(bot_id,event_id,topic,operation_id,payload)
        VALUES (NEW.bot_id,'config:'||NEW.operation_id,
                CASE WHEN activation.recover_effective THEN 'config.recovered' ELSE 'config.effective' END,
                NEW.operation_id,jsonb_build_object('component_id',NEW.component_id,'revision',activation.revision));
    RETURN NEW;
END;
$$;
