-- A compatible replacement may take over only untouched frozen Handler items.
-- The original chain stays immutable; every generation change is an append-only fact.
DO $$
BEGIN
 IF NOT EXISTS(SELECT 1 FROM public.bot_schema_writer_gate
               WHERE singleton AND minimum_generation>=25) THEN
  RAISE EXCEPTION 'WRITER_GATE_NOT_INSTALLED' USING ERRCODE='55000';
 END IF;
END;
$$;

-- Component configuration carries the administrator side of the two-key opt-in.
CREATE OR REPLACE FUNCTION public.forest_component_values_metadata(value jsonb) RETURNS jsonb
LANGUAGE plpgsql IMMUTABLE AS $$
DECLARE result jsonb;
BEGIN
 IF value IS NULL OR jsonb_typeof(value) IS DISTINCT FROM 'object'
  OR NOT(value ?& ARRAY['package_digest','config','grants'])
  OR value-ARRAY['package_digest','config','grants','message_handler_takeover']<>'{}'::jsonb
  OR jsonb_typeof(value->'package_digest') IS DISTINCT FROM 'string'
  OR length(value->>'package_digest') NOT BETWEEN 1 AND 256
  OR jsonb_typeof(value->'config') IS DISTINCT FROM 'object'
  OR jsonb_typeof(value->'grants') IS DISTINCT FROM 'array'
  OR (value ? 'message_handler_takeover'
      AND jsonb_typeof(value->'message_handler_takeover') IS DISTINCT FROM 'boolean') THEN
  RAISE EXCEPTION 'CONFIGURATION_VALUES_INVALID' USING ERRCODE='23514';
 END IF;
 IF jsonb_array_length(value->'grants')>256 OR EXISTS(
  SELECT 1 FROM jsonb_array_elements(value->'grants') g
  WHERE jsonb_typeof(g) IS DISTINCT FROM 'string' OR length(g#>>'{}') NOT BETWEEN 1 AND 128)
  OR (SELECT count(*)<>count(DISTINCT g) FROM jsonb_array_elements(value->'grants') g) THEN
  RAISE EXCEPTION 'CONFIGURATION_VALUES_INVALID' USING ERRCODE='23514';
 END IF;
 result:=jsonb_build_object('package_digest',value->'package_digest','grants',value->'grants');
 IF value ? 'message_handler_takeover' THEN
  result:=result||jsonb_build_object('message_handler_takeover',
                                    value->'message_handler_takeover');
 END IF;
 RETURN result;
END $$;

CREATE OR REPLACE FUNCTION public.forest_component_metadata_valid(value jsonb) RETURNS boolean
LANGUAGE plpgsql IMMUTABLE AS $$
BEGIN
 IF value IS NULL OR jsonb_typeof(value) IS DISTINCT FROM 'object'
  OR NOT(value ?& ARRAY['package_digest','grants'])
  OR value-ARRAY['package_digest','grants','message_handler_takeover']<>'{}'::jsonb THEN
  RETURN false;
 END IF;
 RETURN public.forest_component_values_metadata(value||'{"config":{}}'::jsonb)=value;
EXCEPTION WHEN check_violation THEN RETURN false;
END $$;

ALTER TABLE public.bot_message_handler_generations
 ADD COLUMN takeover_mode text NOT NULL DEFAULT 'none',
 ADD CONSTRAINT bot_message_handler_generation_takeover_check
 CHECK(takeover_mode IN ('none','compatible')
       AND (takeover_mode='none' OR handler_count>0));
ALTER TABLE public.bot_message_handler_generations
 ALTER COLUMN takeover_mode DROP DEFAULT;

CREATE TABLE public.bot_message_handler_item_takeovers (
 bot_id text NOT NULL,
 takeover_id text NOT NULL,
 chain_id text NOT NULL,
 route_index integer NOT NULL CHECK(route_index>=0),
 takeover_no integer NOT NULL CHECK(takeover_no>0),
 component_id text NOT NULL,
 handler_id text NOT NULL,
 spec_digest text NOT NULL CHECK(spec_digest ~ '^sha256:[a-f0-9]{64}$'),
 from_generation_id text NOT NULL,
 from_package_digest text NOT NULL CHECK(from_package_digest ~ '^sha256:[a-f0-9]{64}$'),
 from_route_epoch bigint NOT NULL CHECK(from_route_epoch>0),
 from_registry_revision bigint NOT NULL CHECK(from_registry_revision>0),
 to_generation_id text NOT NULL,
 to_package_digest text NOT NULL CHECK(to_package_digest ~ '^sha256:[a-f0-9]{64}$'),
 to_route_epoch bigint NOT NULL CHECK(to_route_epoch>0),
 to_registry_revision bigint NOT NULL CHECK(to_registry_revision>0),
 lifecycle_operation_id text NOT NULL,
 created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
 PRIMARY KEY(bot_id,chain_id,route_index,takeover_no),
 UNIQUE(bot_id,takeover_id),
 UNIQUE(bot_id,chain_id,route_index,to_generation_id),
 FOREIGN KEY(bot_id,chain_id,route_index)
  REFERENCES public.bot_message_handler_chain_items(bot_id,chain_id,route_index),
 FOREIGN KEY(bot_id,from_generation_id,handler_id)
  REFERENCES public.bot_generation_message_handler_definitions(bot_id,generation_id,handler_id),
 FOREIGN KEY(bot_id,to_generation_id,handler_id)
  REFERENCES public.bot_generation_message_handler_definitions(bot_id,generation_id,handler_id),
 FOREIGN KEY(bot_id,lifecycle_operation_id)
  REFERENCES public.bot_lifecycle_operations(bot_id,operation_id),
 CHECK(from_generation_id<>to_generation_id)
);
CREATE INDEX bot_message_handler_takeover_target
 ON public.bot_message_handler_item_takeovers(bot_id,to_generation_id,chain_id,route_index);

CREATE FUNCTION public.forest_message_handler_takeover_guard() RETURNS trigger
LANGUAGE plpgsql AS $$
DECLARE
 chain record; item record; previous record; operation record;
 source_definition record; target_definition record; target_generation record;
 publication record; expected_id text;
BEGIN
 SELECT * INTO chain FROM public.bot_message_handler_chains
  WHERE bot_id=NEW.bot_id AND chain_id=NEW.chain_id FOR UPDATE;
 SELECT * INTO item FROM public.bot_message_handler_chain_items
  WHERE (bot_id,chain_id,route_index)=(NEW.bot_id,NEW.chain_id,NEW.route_index) FOR SHARE;
 SELECT * INTO previous FROM public.bot_message_handler_item_takeovers
  WHERE (bot_id,chain_id,route_index)=(NEW.bot_id,NEW.chain_id,NEW.route_index)
  ORDER BY takeover_no DESC LIMIT 1;
 SELECT * INTO operation FROM public.bot_lifecycle_operations
  WHERE bot_id=NEW.bot_id AND operation_id=NEW.lifecycle_operation_id;
 SELECT d.*,g.package_digest INTO source_definition
  FROM public.bot_generation_message_handler_definitions d
  JOIN public.bot_message_handler_generations g
   ON (g.bot_id,g.generation_id)=(d.bot_id,d.generation_id)
  WHERE (d.bot_id,d.generation_id,d.handler_id)=
   (NEW.bot_id,NEW.from_generation_id,NEW.handler_id);
 SELECT d.*,g.package_digest,g.takeover_mode INTO target_definition
  FROM public.bot_generation_message_handler_definitions d
  JOIN public.bot_message_handler_generations g
   ON (g.bot_id,g.generation_id)=(d.bot_id,d.generation_id)
  WHERE (d.bot_id,d.generation_id,d.handler_id)=
   (NEW.bot_id,NEW.to_generation_id,NEW.handler_id);
 SELECT * INTO target_generation FROM public.bot_generations
  WHERE bot_id=NEW.bot_id AND generation_id=NEW.to_generation_id;
 SELECT * INTO publication FROM public.bot_message_handler_publications
  WHERE bot_id=NEW.bot_id AND registry_revision=NEW.to_registry_revision;
 expected_id:='message-handler-takeover:sha256:'||encode(sha256(convert_to(
  replace(jsonb_build_array(NEW.chain_id,NEW.route_index,NEW.takeover_no,
                            NEW.to_generation_id)::text,', ',','),'UTF8')),'hex');

 IF chain.bot_id IS NULL OR item.bot_id IS NULL
  OR chain.status<>'PENDING' OR chain.claim_actor_id IS NOT NULL
  OR NEW.route_index<chain.current_index
  OR EXISTS(SELECT 1 FROM public.bot_message_handler_attempts a
            WHERE (a.bot_id,a.chain_id,a.route_index)=
                  (NEW.bot_id,NEW.chain_id,NEW.route_index))
  OR NEW.takeover_no<>COALESCE(previous.takeover_no,0)+1
  OR ROW(NEW.from_generation_id,NEW.from_package_digest,NEW.from_route_epoch,
         NEW.from_registry_revision) IS DISTINCT FROM
     ROW(COALESCE(previous.to_generation_id,item.generation_id),
         COALESCE(previous.to_package_digest,item.package_digest),
         COALESCE(previous.to_route_epoch,item.route_epoch),
         COALESCE(previous.to_registry_revision,item.route_registry_revision))
  OR ROW(NEW.component_id,NEW.handler_id,NEW.spec_digest) IS DISTINCT FROM
     ROW(item.component_id,item.handler_id,item.spec_digest)
  OR ROW(source_definition.component_id,source_definition.spec_digest,
         source_definition.package_digest) IS DISTINCT FROM
     ROW(NEW.component_id,NEW.spec_digest,NEW.from_package_digest)
  OR ROW(target_definition.component_id,target_definition.spec_digest,
         target_definition.package_digest,target_definition.takeover_mode) IS DISTINCT FROM
     ROW(NEW.component_id,NEW.spec_digest,NEW.to_package_digest,'compatible'::text)
  OR operation.bot_id IS NULL OR operation.operation_kind<>'INSTALL'
  OR operation.phase<>'QUIESCENT'
  OR operation.component_id<>NEW.component_id
  OR operation.old_generation_id IS DISTINCT FROM NEW.from_generation_id
  OR operation.candidate_generation_id IS DISTINCT FROM NEW.to_generation_id
  OR operation.expected_route_epoch+1<>NEW.to_route_epoch
  OR target_generation.status<>'READY'
  OR ROW(publication.component_id,publication.generation_id,publication.route_epoch,
         publication.operation_kind) IS DISTINCT FROM
     ROW(NEW.component_id,NEW.to_generation_id,NEW.to_route_epoch,'INSTALL'::text)
  OR NEW.takeover_id<>expected_id THEN
  RAISE EXCEPTION 'MESSAGE_HANDLER_TAKEOVER_PROOF_INVALID' USING ERRCODE='55000';
 END IF;
 RETURN NEW;
END;
$$;
CREATE TRIGGER bot_message_handler_takeover_guard
 BEFORE INSERT ON public.bot_message_handler_item_takeovers
 FOR EACH ROW EXECUTE FUNCTION public.forest_message_handler_takeover_guard();
CREATE TRIGGER bot_message_handler_takeover_immutable
 BEFORE UPDATE OR DELETE ON public.bot_message_handler_item_takeovers
 FOR EACH ROW EXECUTE FUNCTION public.forest_message_handler_fact_immutable_guard();

-- Result receipts compare the call with the latest effective generation while
-- preserving the immutable route and policy snapshot stored in the chain item.
CREATE OR REPLACE FUNCTION public.forest_message_handler_result_guard() RETURNS trigger
LANGUAGE plpgsql AS $$
DECLARE chain record; attempt record; item record; takeover record; call record;
 context record; conversation record; context_valid boolean := true;
BEGIN
 SELECT * INTO chain FROM public.bot_message_handler_chains
  WHERE bot_id=NEW.bot_id AND chain_id=NEW.chain_id;
 SELECT * INTO attempt FROM public.bot_message_handler_attempts
  WHERE (bot_id,chain_id,attempt_no)=(NEW.bot_id,NEW.chain_id,NEW.attempt_no);
 SELECT * INTO item FROM public.bot_message_handler_chain_items
  WHERE (bot_id,chain_id,route_index)=(NEW.bot_id,NEW.chain_id,NEW.route_index);
 SELECT * INTO takeover FROM public.bot_message_handler_item_takeovers
  WHERE (bot_id,chain_id,route_index)=(NEW.bot_id,NEW.chain_id,NEW.route_index)
  ORDER BY takeover_no DESC LIMIT 1;
 SELECT * INTO call FROM public.bot_calls
  WHERE bot_id=NEW.bot_id AND operation_id=NEW.call_operation_id;
 SELECT * INTO conversation FROM public.bot_conversations
  WHERE bot_id=NEW.bot_id AND conversation_id=NEW.conversation_id;
 IF NEW.context_payload_id IS NOT NULL THEN
  SELECT * INTO context FROM public.bot_payload_headers
   WHERE bot_id=NEW.bot_id AND payload_id=NEW.context_payload_id;
  context_valid := context.bot_id IS NOT NULL AND context.owner_kind='context.parts'
   AND context.owner_id=NEW.result_id AND context.digest=NEW.context_payload_digest
   AND context.state='ACTIVE' AND EXISTS(
    SELECT 1 FROM public.bot_payload_derivations d
    WHERE d.bot_id=NEW.bot_id AND d.payload_id=NEW.context_payload_id
     AND d.parent_payload_id=NEW.call_result_payload_id);
 END IF;
 IF chain.bot_id IS NULL OR chain.status<>'PENDING'
  OR chain.current_index<>NEW.route_index OR chain.attempt_count<>NEW.attempt_no
  OR ROW(chain.claim_actor_id,chain.claim_actor_epoch,chain.claim_host_incarnation)
     IS DISTINCT FROM ROW(conversation.actor_id,conversation.actor_epoch,
                          conversation.actor_host_incarnation)
  OR attempt.bot_id IS NULL OR attempt.route_index<>NEW.route_index
  OR NEW.call_operation_id<>attempt.call_operation_id
  OR call.bot_id IS NULL OR call.status<>'COMPLETED'
  OR ROW(call.component_id,call.generation_id,call.route_epoch,
         call.result_payload_id,call.result_digest) IS DISTINCT FROM
     ROW(item.component_id,COALESCE(takeover.to_generation_id,item.generation_id),
         COALESCE(takeover.to_route_epoch,item.route_epoch),
         NEW.call_result_payload_id,NEW.call_result_digest)
  OR NOT context_valid THEN
  RAISE EXCEPTION 'MESSAGE_HANDLER_RESULT_PROOF_INVALID' USING ERRCODE='55000';
 END IF;
 RETURN NEW;
END;
$$;
