-- One Conversation Actor claims a command and atomically publishes its result.
DO $$
BEGIN
 IF NOT EXISTS(SELECT 1 FROM public.bot_schema_writer_gate
               WHERE singleton AND minimum_generation>=14) THEN
  RAISE EXCEPTION 'WRITER_GATE_NOT_INSTALLED' USING ERRCODE='55000';
 END IF;
END;
$$;

ALTER TABLE public.bot_context_input_batches
 DROP CONSTRAINT bot_context_input_batches_kind_check;
ALTER TABLE public.bot_context_input_batches
 ADD CONSTRAINT bot_context_input_batches_kind_check CHECK(kind IN (
  'TRIGGER_INPUT','SILENT_DRAIN','RECOVERY_CLAIM','BUSINESS_COMMAND_INPUT'
 ));

ALTER TABLE public.bot_business_command_requests
 ADD COLUMN claim_actor_id text,
 ADD COLUMN claim_actor_epoch bigint,
 ADD COLUMN claim_host_incarnation text,
 ADD COLUMN claim_input_batch_id text,
 ADD FOREIGN KEY(bot_id,claim_input_batch_id)
  REFERENCES public.bot_context_input_batches(bot_id,input_batch_id),
 ADD CHECK((claim_actor_id IS NULL AND claim_actor_epoch IS NULL
            AND claim_host_incarnation IS NULL AND claim_input_batch_id IS NULL)
        OR (claim_actor_id IS NOT NULL AND claim_actor_epoch>0
            AND claim_host_incarnation IS NOT NULL AND claim_input_batch_id IS NOT NULL)),
 ADD CHECK(status NOT IN ('CLAIMED','COMPLETED') OR claim_actor_id IS NOT NULL);

CREATE OR REPLACE FUNCTION public.forest_business_command_request_guard() RETURNS trigger
LANGUAGE plpgsql AS $$
DECLARE inbox record; arguments record; policy record; conversation record;
BEGIN
 IF TG_OP='DELETE' THEN
  RAISE EXCEPTION 'BUSINESS_COMMAND_REQUEST_IMMUTABLE' USING ERRCODE='55000';
 END IF;
 IF TG_OP='UPDATE' THEN
  IF (to_jsonb(NEW)-ARRAY['status','terminal_reason','completed_at','claim_actor_id',
       'claim_actor_epoch','claim_host_incarnation','claim_input_batch_id']) IS DISTINCT FROM
     (to_jsonb(OLD)-ARRAY['status','terminal_reason','completed_at','claim_actor_id',
       'claim_actor_epoch','claim_host_incarnation','claim_input_batch_id'])
   OR OLD.status IN ('COMPLETED','DENIED','FAILED','CANCELLED')
   OR NOT ((OLD.status='PENDING' AND NEW.status IN ('CLAIMED','FAILED','CANCELLED'))
        OR (OLD.status='CLAIMED' AND NEW.status IN ('CLAIMED','COMPLETED','FAILED'))) THEN
   RAISE EXCEPTION 'BUSINESS_COMMAND_REQUEST_IMMUTABLE' USING ERRCODE='55000';
  END IF;
  IF NEW.status IN ('CLAIMED','COMPLETED')
     OR (NEW.status='FAILED' AND NEW.claim_actor_id IS NOT NULL) THEN
   SELECT * INTO conversation FROM public.bot_conversations
    WHERE bot_id=NEW.bot_id AND conversation_id=NEW.conversation_id;
   IF conversation.bot_id IS NULL OR
      ROW(conversation.actor_id,conversation.actor_epoch,conversation.actor_host_incarnation) IS DISTINCT FROM
      ROW(NEW.claim_actor_id,NEW.claim_actor_epoch,NEW.claim_host_incarnation) THEN
    RAISE EXCEPTION 'BUSINESS_COMMAND_ACTOR_FENCE' USING ERRCODE='55000';
   END IF;
  END IF;
  IF OLD.status='CLAIMED' AND NEW.status='COMPLETED' AND NOT EXISTS(
      SELECT 1 FROM public.bot_business_command_results r
       WHERE r.bot_id=NEW.bot_id AND r.request_id=NEW.request_id
        AND r.disposition IN ('REPLY','CONSUMED')) THEN
   RAISE EXCEPTION 'BUSINESS_COMMAND_RESULT_REQUIRED' USING ERRCODE='55000';
  ELSIF OLD.status='CLAIMED' AND NEW.status='FAILED'
    AND NEW.terminal_reason='COMMAND_NOT_HANDLED' AND NOT EXISTS(
      SELECT 1 FROM public.bot_business_command_results r
       WHERE r.bot_id=NEW.bot_id AND r.request_id=NEW.request_id
        AND r.disposition='PASS') THEN
   RAISE EXCEPTION 'BUSINESS_COMMAND_RESULT_REQUIRED' USING ERRCODE='55000';
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

CREATE TABLE public.bot_business_command_results (
 bot_id text NOT NULL,
 result_id text NOT NULL,
 operation_id text NOT NULL,
 request_id text NOT NULL,
 conversation_id text NOT NULL,
 disposition text NOT NULL CHECK(disposition IN ('REPLY','CONSUMED','PASS')),
 call_operation_id text NOT NULL,
 call_result_payload_id text NOT NULL,
 call_result_digest text NOT NULL CHECK(call_result_digest ~ '^sha256:[a-f0-9]{64}$'),
 context_payload_id text NOT NULL,
 context_payload_digest text NOT NULL CHECK(context_payload_digest ~ '^sha256:[a-f0-9]{64}$'),
 result_digest text NOT NULL CHECK(result_digest ~ '^sha256:[a-f0-9]{64}$'),
 response_seq bigint,
 delivery jsonb,
 terminal_reason text,
 created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
 PRIMARY KEY(bot_id,result_id),
 UNIQUE(bot_id,request_id),
 UNIQUE(bot_id,conversation_id,operation_id),
 FOREIGN KEY(bot_id,request_id) REFERENCES public.bot_business_command_requests(bot_id,request_id),
 FOREIGN KEY(bot_id,conversation_id) REFERENCES public.bot_conversations(bot_id,conversation_id),
 FOREIGN KEY(bot_id,call_result_payload_id) REFERENCES public.bot_payload_headers(bot_id,payload_id),
 FOREIGN KEY(bot_id,context_payload_id) REFERENCES public.bot_payload_headers(bot_id,payload_id),
 CHECK((disposition='REPLY')=(response_seq IS NOT NULL AND delivery IS NOT NULL)),
 CHECK((disposition='PASS')=(terminal_reason IS NOT NULL))
);
CREATE UNIQUE INDEX bot_business_command_result_response
 ON public.bot_business_command_results(bot_id,conversation_id,response_seq)
 WHERE response_seq IS NOT NULL;
CREATE FUNCTION public.forest_business_command_result_guard() RETURNS trigger
LANGUAGE plpgsql AS $$
DECLARE request record; call record; output record; context record; conversation record;
BEGIN
 SELECT * INTO request FROM public.bot_business_command_requests
  WHERE bot_id=NEW.bot_id AND request_id=NEW.request_id;
 SELECT * INTO call FROM public.bot_calls
  WHERE bot_id=NEW.bot_id AND operation_id=NEW.call_operation_id;
 SELECT * INTO output FROM public.bot_payload_headers
  WHERE bot_id=NEW.bot_id AND payload_id=NEW.call_result_payload_id;
 SELECT * INTO context FROM public.bot_payload_headers
  WHERE bot_id=NEW.bot_id AND payload_id=NEW.context_payload_id;
 SELECT * INTO conversation FROM public.bot_conversations
  WHERE bot_id=NEW.bot_id AND conversation_id=NEW.conversation_id;
 IF request.bot_id IS NULL OR request.status<>'CLAIMED'
  OR request.conversation_id<>NEW.conversation_id
  OR ROW(request.claim_actor_id,request.claim_actor_epoch,request.claim_host_incarnation)
     IS DISTINCT FROM
     ROW(conversation.actor_id,conversation.actor_epoch,conversation.actor_host_incarnation)
  OR call.bot_id IS NULL OR call.status<>'COMPLETED'
  OR ROW(call.component_id,call.generation_id,call.route_epoch,
         call.result_payload_id,call.result_digest) IS DISTINCT FROM
     ROW(request.component_id,request.generation_id,request.route_epoch,
         NEW.call_result_payload_id,NEW.call_result_digest)
  OR output.bot_id IS NULL OR output.owner_kind<>'kernel.result'
  OR output.digest<>NEW.call_result_digest OR output.state<>'ACTIVE'
  OR context.bot_id IS NULL OR context.owner_kind<>'context.parts'
  OR context.owner_id<>NEW.result_id OR context.digest<>NEW.context_payload_digest
  OR context.state<>'ACTIVE'
  OR NOT EXISTS(SELECT 1 FROM public.bot_payload_derivations d
      WHERE d.bot_id=NEW.bot_id AND d.payload_id=NEW.context_payload_id
       AND d.parent_payload_id=NEW.call_result_payload_id) THEN
  RAISE EXCEPTION 'BUSINESS_COMMAND_RESULT_PROOF_INVALID' USING ERRCODE='55000';
 END IF;
 RETURN NEW;
END;
$$;
CREATE TRIGGER bot_business_command_result_guard
 BEFORE INSERT ON public.bot_business_command_results
 FOR EACH ROW EXECUTE FUNCTION public.forest_business_command_result_guard();
CREATE TRIGGER bot_business_command_results_immutable
 BEFORE UPDATE OR DELETE ON public.bot_business_command_results
 FOR EACH ROW EXECUTE FUNCTION public.forest_bot_reject_mutation();

ALTER TABLE public.bot_context_units DROP CONSTRAINT bot_context_units_kind_check;
ALTER TABLE public.bot_context_units
 ADD CONSTRAINT bot_context_units_kind_check CHECK(kind IN (
  'INPUT','REPLY','CONTROL','RUN_STEP','COMMAND_RESULT'
 )),
 ADD COLUMN business_request_id text,
 ADD UNIQUE(bot_id,business_request_id),
 ADD FOREIGN KEY(bot_id,business_request_id)
  REFERENCES public.bot_business_command_results(bot_id,request_id),
 ADD CHECK((kind='COMMAND_RESULT')=(business_request_id IS NOT NULL));
