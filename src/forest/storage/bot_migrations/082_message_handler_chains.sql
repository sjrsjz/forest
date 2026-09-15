-- Freeze ordinary-message routing and Agent fallback before classification can advance.
DO $$
BEGIN
 IF NOT EXISTS(SELECT 1 FROM public.bot_schema_writer_gate
               WHERE singleton AND minimum_generation>=21) THEN
  RAISE EXCEPTION 'WRITER_GATE_NOT_INSTALLED' USING ERRCODE='55000';
 END IF;
END;
$$;

CREATE TABLE public.bot_message_handler_chains (
 bot_id text NOT NULL,
 chain_id text NOT NULL,
 event_id text NOT NULL,
 conversation_id text NOT NULL,
 binding_id text NOT NULL,
 ingress_seq bigint NOT NULL CHECK(ingress_seq>0),
 principal_ref text NOT NULL,
 external_user_id text NOT NULL,
 input_payload_id text NOT NULL,
 registry_revision bigint NOT NULL CHECK(registry_revision>=0),
 route_count integer NOT NULL CHECK(route_count BETWEEN 0 AND 256),
 route_set_digest text NOT NULL CHECK(route_set_digest ~ '^sha256:[a-f0-9]{64}$'),
 fallback_decision text NOT NULL CHECK(fallback_decision IN ('PENDING','TRIGGER','NO_TRIGGER')),
 fallback_rule_revision text,
 fallback_reason text,
 fallback_digest text CHECK(fallback_digest IS NULL OR fallback_digest ~ '^sha256:[a-f0-9]{64}$'),
 status text NOT NULL CHECK(status IN ('PENDING','PASSED','CONSUMED','REPLIED','FAILED','CANCELLED')),
 current_index integer NOT NULL DEFAULT 0 CHECK(current_index>=0),
 revision bigint NOT NULL DEFAULT 1 CHECK(revision>0),
 deadline_at timestamptz NOT NULL,
 terminal_reason text,
 created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
 completed_at timestamptz,
 PRIMARY KEY(bot_id,chain_id),
 UNIQUE(bot_id,event_id),
 UNIQUE(bot_id,conversation_id,ingress_seq),
 FOREIGN KEY(bot_id,event_id) REFERENCES public.bot_message_inbox(bot_id,event_id),
 FOREIGN KEY(bot_id,conversation_id) REFERENCES public.bot_conversations(bot_id,conversation_id),
 FOREIGN KEY(bot_id,binding_id) REFERENCES public.bot_bindings(bot_id,binding_id),
 FOREIGN KEY(bot_id,input_payload_id) REFERENCES public.bot_payload_headers(bot_id,payload_id),
 CHECK((fallback_decision='PENDING')=(fallback_digest IS NULL)),
 CHECK((fallback_decision='PENDING')=(fallback_rule_revision IS NULL)),
 CHECK((fallback_decision='PENDING')=(fallback_reason IS NULL)),
 CHECK(current_index<=route_count),
 CHECK((status='PENDING' AND route_count>0 AND completed_at IS NULL)
    OR (status='PASSED' AND route_count=0 AND current_index=0 AND completed_at IS NOT NULL)
    OR (status IN ('CONSUMED','REPLIED','FAILED','CANCELLED') AND completed_at IS NOT NULL))
);
CREATE INDEX bot_message_handler_chain_pending
 ON public.bot_message_handler_chains(bot_id,conversation_id,status,ingress_seq)
 WHERE status='PENDING';

CREATE TABLE public.bot_message_handler_chain_items (
 bot_id text NOT NULL,
 chain_id text NOT NULL,
 route_index integer NOT NULL CHECK(route_index>=0),
 binding_id text NOT NULL,
 component_id text NOT NULL,
 handler_id text NOT NULL,
 handler text NOT NULL,
 priority integer NOT NULL CHECK(priority BETWEEN -100 AND 100),
 generation_id text NOT NULL,
 package_digest text NOT NULL CHECK(package_digest ~ '^sha256:[a-f0-9]{64}$'),
 route_epoch bigint NOT NULL CHECK(route_epoch>0),
 route_registry_revision bigint NOT NULL CHECK(route_registry_revision>0),
 spec_digest text NOT NULL CHECK(spec_digest ~ '^sha256:[a-f0-9]{64}$'),
 policy_revision bigint NOT NULL CHECK(policy_revision>0),
 policy_mode text NOT NULL CHECK(policy_mode IN ('EVERYONE','ADMINS','USER_SET')),
 policy_payload_id text NOT NULL,
 policy_payload_digest text NOT NULL CHECK(policy_payload_digest ~ '^sha256:[a-f0-9]{64}$'),
 binding_config_revision bigint,
 PRIMARY KEY(bot_id,chain_id,route_index),
 UNIQUE(bot_id,chain_id,component_id,handler_id),
 FOREIGN KEY(bot_id,chain_id) REFERENCES public.bot_message_handler_chains(bot_id,chain_id),
 FOREIGN KEY(bot_id,generation_id,handler_id)
  REFERENCES public.bot_generation_message_handler_definitions(bot_id,generation_id,handler_id),
 FOREIGN KEY(bot_id,binding_id,component_id,handler_id,policy_revision)
  REFERENCES public.bot_message_handler_policy_revisions(bot_id,binding_id,component_id,handler_id,revision)
  DEFERRABLE INITIALLY DEFERRED,
 FOREIGN KEY(bot_id,policy_payload_id) REFERENCES public.bot_payload_headers(bot_id,payload_id)
);

CREATE FUNCTION public.forest_message_handler_chain_guard() RETURNS trigger
LANGUAGE plpgsql AS $$
DECLARE inbox record;
BEGIN
 IF TG_OP='INSERT' THEN
  SELECT * INTO inbox FROM public.bot_message_inbox
   WHERE bot_id=NEW.bot_id AND event_id=NEW.event_id;
  IF inbox.bot_id IS NULL OR inbox.trigger_decision<>'PENDING'
   OR ROW(inbox.conversation_id,inbox.ingress_seq,inbox.payload_id,
          inbox.author->>'principal_ref',inbox.author->>'external_user_id') IS DISTINCT FROM
      ROW(NEW.conversation_id,NEW.ingress_seq,NEW.input_payload_id,
          NEW.principal_ref,NEW.external_user_id)
   OR NEW.chain_id<>'message-handler-chain:sha256:' || encode(sha256(convert_to(
      to_jsonb(NEW.event_id)::text,'UTF8')),'hex') THEN
   RAISE EXCEPTION 'MESSAGE_HANDLER_CHAIN_PROOF_INVALID' USING ERRCODE='55000';
  END IF;
  RETURN NEW;
 END IF;
 IF TG_OP='DELETE' THEN
  RAISE EXCEPTION 'MESSAGE_HANDLER_CHAIN_DELETE_FORBIDDEN' USING ERRCODE='55000';
 END IF;
 IF (to_jsonb(NEW)-ARRAY['fallback_decision','fallback_rule_revision','fallback_reason',
      'fallback_digest','status','current_index','revision','terminal_reason','completed_at'])
    IS DISTINCT FROM
    (to_jsonb(OLD)-ARRAY['fallback_decision','fallback_rule_revision','fallback_reason',
      'fallback_digest','status','current_index','revision','terminal_reason','completed_at'])
  OR NEW.revision<>OLD.revision+1 THEN
  RAISE EXCEPTION 'MESSAGE_HANDLER_CHAIN_IMMUTABLE' USING ERRCODE='55000';
 END IF;
 IF OLD.fallback_decision='PENDING' AND NEW.status=OLD.status
    AND NEW.current_index=OLD.current_index AND NEW.terminal_reason IS NOT DISTINCT FROM OLD.terminal_reason
    AND NEW.completed_at IS NOT DISTINCT FROM OLD.completed_at THEN
  IF NEW.fallback_decision NOT IN ('TRIGGER','NO_TRIGGER') THEN
   RAISE EXCEPTION 'MESSAGE_HANDLER_FALLBACK_INVALID' USING ERRCODE='55000';
  END IF;
  RETURN NEW;
 END IF;
 IF NEW.fallback_decision IS DISTINCT FROM OLD.fallback_decision
   OR NEW.fallback_rule_revision IS DISTINCT FROM OLD.fallback_rule_revision
   OR NEW.fallback_reason IS DISTINCT FROM OLD.fallback_reason
   OR NEW.fallback_digest IS DISTINCT FROM OLD.fallback_digest
   OR NEW.current_index<>OLD.current_index
   OR OLD.status NOT IN ('PENDING','PASSED') OR NEW.status<>'CANCELLED'
   OR NEW.terminal_reason NOT IN ('BUSINESS_COMMAND_OWNED','BUSINESS_CLASSIFICATION_FAILED',
      'POLICY_REVOKED') OR NEW.completed_at IS NULL THEN
  RAISE EXCEPTION 'MESSAGE_HANDLER_CHAIN_TRANSITION_INVALID' USING ERRCODE='55000';
 END IF;
 RETURN NEW;
END;
$$;
CREATE TRIGGER bot_message_handler_chain_guard
 BEFORE INSERT OR UPDATE OR DELETE ON public.bot_message_handler_chains
 FOR EACH ROW EXECUTE FUNCTION public.forest_message_handler_chain_guard();

CREATE FUNCTION public.forest_message_handler_chain_item_guard() RETURNS trigger
LANGUAGE plpgsql AS $$
DECLARE chain record; definition record; policy record;
BEGIN
 IF TG_OP<>'INSERT' THEN
  RAISE EXCEPTION 'MESSAGE_HANDLER_CHAIN_ITEM_IMMUTABLE' USING ERRCODE='55000';
 END IF;
 SELECT * INTO chain FROM public.bot_message_handler_chains
  WHERE bot_id=NEW.bot_id AND chain_id=NEW.chain_id;
 SELECT d.*,g.package_digest INTO definition
  FROM public.bot_generation_message_handler_definitions d
  JOIN public.bot_message_handler_generations g
   ON (g.bot_id,g.generation_id)=(d.bot_id,d.generation_id)
  WHERE (d.bot_id,d.generation_id,d.handler_id)=(NEW.bot_id,NEW.generation_id,NEW.handler_id);
 SELECT * INTO policy FROM public.bot_message_handler_policy_revisions
  WHERE (bot_id,binding_id,component_id,handler_id,revision)=
   (NEW.bot_id,NEW.binding_id,NEW.component_id,NEW.handler_id,NEW.policy_revision);
 IF chain.bot_id IS NULL OR NEW.binding_id<>chain.binding_id
  OR NEW.route_index>=chain.route_count OR NEW.route_registry_revision>chain.registry_revision
  OR ROW(definition.component_id,definition.handler,definition.priority,definition.spec_digest,
         definition.package_digest) IS DISTINCT FROM
     ROW(NEW.component_id,NEW.handler,NEW.priority,NEW.spec_digest,NEW.package_digest)
  OR ROW(policy.policy_mode,policy.values_payload_id,policy.values_payload_digest) IS DISTINCT FROM
     ROW(NEW.policy_mode,NEW.policy_payload_id,NEW.policy_payload_digest) THEN
  RAISE EXCEPTION 'MESSAGE_HANDLER_CHAIN_ITEM_PROOF_INVALID' USING ERRCODE='55000';
 END IF;
 RETURN NEW;
END;
$$;
CREATE TRIGGER bot_message_handler_chain_item_guard
 BEFORE INSERT OR UPDATE OR DELETE ON public.bot_message_handler_chain_items
 FOR EACH ROW EXECUTE FUNCTION public.forest_message_handler_chain_item_guard();

CREATE FUNCTION public.forest_message_handler_chain_complete_guard() RETURNS trigger
LANGUAGE plpgsql AS $$
DECLARE selected record; actual_count integer; ordered_digest text;
BEGIN
 IF TG_TABLE_NAME='bot_message_handler_chains' THEN
  SELECT * INTO selected FROM public.bot_message_handler_chains
   WHERE bot_id=NEW.bot_id AND chain_id=NEW.chain_id;
 ELSE
  SELECT * INTO selected FROM public.bot_message_handler_chains
   WHERE bot_id=NEW.bot_id AND chain_id=NEW.chain_id;
 END IF;
 SELECT count(*), 'sha256:' || encode(sha256(convert_to(COALESCE(jsonb_agg(
  jsonb_build_object('route_index',route_index,'component_id',component_id,
   'handler_id',handler_id,'handler',handler,'priority',priority,'generation_id',generation_id,
   'package_digest',package_digest,'route_epoch',route_epoch,
   'route_registry_revision',route_registry_revision,'spec_digest',spec_digest,
   'policy_revision',policy_revision,'policy_mode',policy_mode,
   'policy_payload_id',policy_payload_id,'policy_payload_digest',policy_payload_digest,
   'binding_config_revision',binding_config_revision)
  ORDER BY route_index)::text,'[]'),'UTF8')),'hex')
 INTO actual_count,ordered_digest FROM public.bot_message_handler_chain_items
 WHERE bot_id=selected.bot_id AND chain_id=selected.chain_id;
 IF actual_count<>selected.route_count OR ordered_digest<>selected.route_set_digest
  OR EXISTS(SELECT 1 FROM generate_series(0,selected.route_count-1) expected
     WHERE NOT EXISTS(SELECT 1 FROM public.bot_message_handler_chain_items i
      WHERE i.bot_id=selected.bot_id AND i.chain_id=selected.chain_id
       AND i.route_index=expected)) THEN
  RAISE EXCEPTION 'MESSAGE_HANDLER_CHAIN_INCOMPLETE' USING ERRCODE='55000';
 END IF;
 RETURN NULL;
END;
$$;
CREATE CONSTRAINT TRIGGER bot_message_handler_chain_complete
 AFTER INSERT OR UPDATE ON public.bot_message_handler_chains
 DEFERRABLE INITIALLY DEFERRED FOR EACH ROW
 EXECUTE FUNCTION public.forest_message_handler_chain_complete_guard();
CREATE CONSTRAINT TRIGGER bot_message_handler_chain_item_complete
 AFTER INSERT ON public.bot_message_handler_chain_items
 DEFERRABLE INITIALLY DEFERRED FOR EACH ROW
 EXECUTE FUNCTION public.forest_message_handler_chain_complete_guard();
