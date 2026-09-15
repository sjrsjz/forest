-- Classification and its effective route/policy proof are one durable fact.
DO $$
BEGIN
 IF NOT EXISTS(SELECT 1 FROM public.bot_schema_writer_gate
               WHERE singleton AND minimum_generation>=13) THEN
  RAISE EXCEPTION 'WRITER_GATE_NOT_INSTALLED' USING ERRCODE='55000';
 END IF;
END;
$$;

ALTER TABLE public.bot_message_inbox DROP CONSTRAINT bot_message_inbox_trigger_decision_check;
ALTER TABLE public.bot_message_inbox ADD CONSTRAINT bot_message_inbox_trigger_decision_check
 CHECK(trigger_decision IN (
  'PENDING','TRIGGER','NO_TRIGGER','CONTROL_COMMAND','BUSINESS_COMMAND','FAILED'
 ));

CREATE TABLE public.bot_business_command_requests (
 bot_id text NOT NULL,
 request_id text NOT NULL,
 event_id text NOT NULL,
 conversation_id text NOT NULL,
 binding_id text NOT NULL,
 ingress_seq bigint NOT NULL CHECK(ingress_seq>0),
 principal_ref text NOT NULL,
 external_user_id text NOT NULL,
 command_id text NOT NULL,
 command_name text NOT NULL,
 component_id text NOT NULL,
 generation_id text NOT NULL,
 route_epoch bigint NOT NULL CHECK(route_epoch>0),
 registry_revision bigint NOT NULL CHECK(registry_revision>0),
 command_spec_digest text NOT NULL CHECK(command_spec_digest ~ '^sha256:[a-f0-9]{64}$'),
 handler text NOT NULL,
 permission text NOT NULL,
 argument_mode text NOT NULL CHECK(argument_mode IN ('none','tail_text')),
 input_schema text NOT NULL,
 schema_digest text NOT NULL CHECK(schema_digest ~ '^sha256:[a-f0-9]{64}$'),
 command_policy_revision bigint NOT NULL CHECK(command_policy_revision>0),
 policy_mode text NOT NULL CHECK(policy_mode IN ('EVERYONE','ADMINS','USER_SET')),
 policy_payload_id text NOT NULL,
 policy_payload_digest text NOT NULL CHECK(policy_payload_digest ~ '^sha256:[a-f0-9]{64}$'),
 binding_config_revision bigint,
 input_payload_id text NOT NULL,
 arguments_payload_id text NOT NULL,
 arguments_payload_digest text NOT NULL CHECK(arguments_payload_digest ~ '^sha256:[a-f0-9]{64}$'),
 arguments_digest text NOT NULL CHECK(arguments_digest ~ '^sha256:[a-f0-9]{64}$'),
 request_digest text NOT NULL CHECK(request_digest ~ '^sha256:[a-f0-9]{64}$'),
 status text NOT NULL CHECK(status IN ('PENDING','CLAIMED','COMPLETED','DENIED','FAILED','CANCELLED')),
 terminal_reason text,
 created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
 completed_at timestamptz,
 PRIMARY KEY(bot_id,request_id),
 UNIQUE(bot_id,event_id),
 FOREIGN KEY(bot_id,event_id) REFERENCES public.bot_message_inbox(bot_id,event_id),
 FOREIGN KEY(bot_id,conversation_id) REFERENCES public.bot_conversations(bot_id,conversation_id),
 FOREIGN KEY(bot_id,binding_id) REFERENCES public.bot_bindings(bot_id,binding_id),
 FOREIGN KEY(bot_id,generation_id,command_id)
  REFERENCES public.bot_generation_command_definitions(bot_id,generation_id,command_id),
 FOREIGN KEY(bot_id,binding_id,command_id,command_policy_revision)
  REFERENCES public.bot_command_policy_revisions(bot_id,binding_id,command_id,revision),
 FOREIGN KEY(bot_id,input_payload_id) REFERENCES public.bot_payload_headers(bot_id,payload_id),
 FOREIGN KEY(bot_id,arguments_payload_id) REFERENCES public.bot_payload_headers(bot_id,payload_id),
 FOREIGN KEY(bot_id,policy_payload_id) REFERENCES public.bot_payload_headers(bot_id,payload_id),
 CHECK((status IN ('DENIED','FAILED','CANCELLED','COMPLETED'))=(completed_at IS NOT NULL)),
 CHECK(status<>'DENIED' OR terminal_reason IS NOT DISTINCT FROM 'BUSINESS_COMMAND_DENIED')
);
CREATE INDEX bot_business_command_pending
 ON public.bot_business_command_requests(bot_id,conversation_id,status,ingress_seq)
 WHERE status IN ('PENDING','CLAIMED');

CREATE FUNCTION public.forest_business_command_request_guard() RETURNS trigger
LANGUAGE plpgsql AS $$
DECLARE inbox record; arguments record; policy record;
BEGIN
 IF TG_OP='DELETE' THEN
  RAISE EXCEPTION 'BUSINESS_COMMAND_REQUEST_IMMUTABLE' USING ERRCODE='55000';
 END IF;
 IF TG_OP='UPDATE' THEN
  IF (to_jsonb(NEW)-ARRAY['status','terminal_reason','completed_at']) IS DISTINCT FROM
     (to_jsonb(OLD)-ARRAY['status','terminal_reason','completed_at'])
   OR OLD.status IN ('COMPLETED','DENIED','FAILED','CANCELLED')
   OR NOT ((OLD.status='PENDING' AND NEW.status IN ('CLAIMED','FAILED','CANCELLED'))
        OR (OLD.status='CLAIMED' AND NEW.status IN ('COMPLETED','FAILED'))) THEN
   RAISE EXCEPTION 'BUSINESS_COMMAND_REQUEST_IMMUTABLE' USING ERRCODE='55000';
  END IF;
  RETURN NEW;
 END IF;
 SELECT * INTO inbox FROM public.bot_message_inbox
  WHERE bot_id=NEW.bot_id AND event_id=NEW.event_id;
 SELECT * INTO arguments FROM public.bot_payload_headers
  WHERE bot_id=NEW.bot_id AND payload_id=NEW.arguments_payload_id;
 SELECT * INTO policy FROM public.bot_command_policy_revisions
  WHERE (bot_id,binding_id,command_id,revision)=
        (NEW.bot_id,NEW.binding_id,NEW.command_id,NEW.command_policy_revision);
 IF inbox.bot_id IS NULL OR inbox.trigger_decision<>'PENDING'
  OR ROW(inbox.conversation_id,inbox.ingress_seq,inbox.payload_id,inbox.author->>'principal_ref',
         inbox.author->>'external_user_id') IS DISTINCT FROM
     ROW(NEW.conversation_id,NEW.ingress_seq,NEW.input_payload_id,NEW.principal_ref,
         NEW.external_user_id)
  OR arguments.bot_id IS NULL OR arguments.owner_kind<>'command.arguments'
  OR arguments.owner_id<>'business-command-arguments-v1:sha256:' || encode(sha256(convert_to(
       '[' || to_jsonb(NEW.request_id)::text || ']','UTF8')),'hex')
  OR arguments.digest<>NEW.arguments_payload_digest
  OR NEW.arguments_digest<>NEW.arguments_payload_digest OR arguments.state<>'ACTIVE'
  OR NOT EXISTS(SELECT 1 FROM public.bot_payload_bodies b
      WHERE b.bot_id=NEW.bot_id AND b.payload_id=NEW.arguments_payload_id
       AND jsonb_typeof(convert_from(b.content,'UTF8')::jsonb)='object'
       AND (convert_from(b.content,'UTF8')::jsonb)->>'request_id'=NEW.request_id
       AND (convert_from(b.content,'UTF8')::jsonb)->>'command_id'=NEW.command_id
       AND (convert_from(b.content,'UTF8')::jsonb)->>'name'=NEW.command_name
       AND (convert_from(b.content,'UTF8')::jsonb)->>'argument_mode'=NEW.argument_mode
       AND jsonb_typeof((convert_from(b.content,'UTF8')::jsonb)->'argument_text')='string')
  OR NOT EXISTS(SELECT 1 FROM public.bot_payload_derivations d
      WHERE d.bot_id=NEW.bot_id AND d.payload_id=NEW.arguments_payload_id
       AND d.parent_payload_id=NEW.input_payload_id)
  OR policy.bot_id IS NULL
  OR ROW(policy.policy_mode,policy.values_payload_id,policy.values_payload_digest) IS DISTINCT FROM
     ROW(NEW.policy_mode,NEW.policy_payload_id,NEW.policy_payload_digest) THEN
  RAISE EXCEPTION 'BUSINESS_COMMAND_REQUEST_PROOF_INVALID' USING ERRCODE='55000';
 END IF;
 RETURN NEW;
END;
$$;
CREATE TRIGGER bot_business_command_request_guard
 BEFORE INSERT OR UPDATE OR DELETE ON public.bot_business_command_requests
 FOR EACH ROW EXECUTE FUNCTION public.forest_business_command_request_guard();

CREATE FUNCTION public.forest_business_command_inbox_guard() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
 IF NEW.trigger_decision='BUSINESS_COMMAND' AND OLD.trigger_decision='PENDING'
  AND NOT EXISTS(SELECT 1 FROM public.bot_business_command_requests r
   WHERE r.bot_id=NEW.bot_id AND r.event_id=NEW.event_id
    AND r.conversation_id=NEW.conversation_id AND r.ingress_seq=NEW.ingress_seq) THEN
  RAISE EXCEPTION 'BUSINESS_COMMAND_REQUEST_MISSING' USING ERRCODE='55000';
 END IF;
 RETURN NEW;
END;
$$;
CREATE TRIGGER bot_business_command_inbox_guard
 BEFORE UPDATE ON public.bot_message_inbox FOR EACH ROW
 EXECUTE FUNCTION public.forest_business_command_inbox_guard();

CREATE FUNCTION public.forest_cancel_revoked_business_command() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
 IF OLD.state='ACTIVE' AND NEW.state='REVOKED' AND NEW.owner_kind='command.arguments' THEN
  UPDATE public.bot_business_command_requests SET status='CANCELLED',
   terminal_reason='CONTROL_CONTENT_REVOKED',completed_at=clock_timestamp()
   WHERE bot_id=NEW.bot_id AND arguments_payload_id=NEW.payload_id AND status='PENDING';
 END IF;
 RETURN NEW;
END;
$$;
CREATE TRIGGER bot_cancel_revoked_business_command
 AFTER UPDATE ON public.bot_payload_headers FOR EACH ROW
 EXECUTE FUNCTION public.forest_cancel_revoked_business_command();
