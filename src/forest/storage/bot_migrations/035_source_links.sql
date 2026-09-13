-- Exact receipt-backed aliases add deletion dependencies without changing any
-- old source manifest, request digest, or previously committed forget target.
CREATE TABLE public.bot_payload_source_links (
 bot_id text NOT NULL,
 link_id text NOT NULL,
 event_id text NOT NULL,
 attempt_id text NOT NULL,
 dependent_source_unit_id text NOT NULL,
 dependent_source_revision bigint NOT NULL CHECK(dependent_source_revision=1),
 parent_payload_id text NOT NULL,
 accepted_deletion_epoch bigint NOT NULL CHECK(accepted_deletion_epoch>=0),
 created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
 PRIMARY KEY(bot_id,event_id), UNIQUE(bot_id,link_id),
 FOREIGN KEY(bot_id,event_id) REFERENCES public.bot_message_echo_aliases(bot_id,event_id),
 FOREIGN KEY(bot_id,attempt_id) REFERENCES public.bot_send_attempts(bot_id,attempt_id),
 FOREIGN KEY(bot_id,parent_payload_id) REFERENCES public.bot_payload_headers(bot_id,payload_id),
 FOREIGN KEY(bot_id,dependent_source_unit_id) REFERENCES public.bot_memory_source_units(bot_id,source_unit_id)
);
CREATE INDEX bot_source_link_parent ON public.bot_payload_source_links(bot_id,parent_payload_id);
CREATE INDEX bot_source_link_dependent ON public.bot_payload_source_links(bot_id,dependent_source_unit_id);
CREATE TRIGGER bot_source_links_immutable BEFORE UPDATE OR DELETE ON public.bot_payload_source_links
 FOR EACH ROW EXECUTE FUNCTION public.forest_bot_reject_mutation();

CREATE FUNCTION public.forest_source_dependency_closure(selected_bot text,initial_sources jsonb,maximum integer)
RETURNS TABLE(source_unit_id text,source_revision bigint,part_id text)
LANGUAGE sql STABLE AS $$
 WITH RECURSIVE dependencies(unit,revision,part) AS (
  SELECT j->>'source_unit_id',(j->>'source_revision')::bigint,j->>'part_id'
   FROM jsonb_array_elements(initial_sources) j
  UNION
  SELECT p.source_unit_id,p.source_revision,p.part_id FROM dependencies d
   JOIN public.bot_payload_source_links l ON l.bot_id=selected_bot AND l.dependent_source_unit_id=d.unit
   JOIN public.bot_payload_sources p ON p.bot_id=l.bot_id AND p.payload_id=l.parent_payload_id
 ) SELECT d.unit,d.revision,d.part FROM dependencies d LIMIT maximum+1
$$;

CREATE FUNCTION public.forest_source_dependent_closure(selected_bot text,initial_targets jsonb,maximum integer)
RETURNS TABLE(source_unit_id text,part_id text)
LANGUAGE sql STABLE AS $$
 WITH RECURSIVE targets(unit,part) AS (
  SELECT j->>'source_unit_id',j->>'part_id' FROM jsonb_array_elements(initial_targets) j
  UNION
  SELECT l.dependent_source_unit_id,'*' FROM targets t
   JOIN public.bot_payload_sources p ON p.bot_id=selected_bot AND p.source_unit_id=t.unit
    AND (p.part_id='*' OR t.part='*' OR p.part_id=t.part)
   JOIN public.bot_payload_source_links l ON l.bot_id=p.bot_id AND l.parent_payload_id=p.payload_id
 ) SELECT t.unit,t.part FROM targets t LIMIT maximum+1
$$;

CREATE FUNCTION public.forest_source_link_guard() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE current_epoch bigint;
BEGIN
 SELECT deletion_epoch INTO current_epoch FROM public.bot_memory_clock WHERE bot_id=NEW.bot_id FOR UPDATE;
 IF current_epoch IS NULL OR NEW.accepted_deletion_epoch<>current_epoch OR NOT EXISTS(
  SELECT 1 FROM public.bot_message_echo_aliases e
   JOIN public.bot_message_inbox i ON i.bot_id=e.bot_id AND i.event_id=e.event_id
   JOIN public.bot_conversations c ON c.bot_id=i.bot_id AND c.conversation_id=i.conversation_id
   JOIN public.bot_bindings b ON b.bot_id=c.bot_id AND b.binding_id=c.binding_id
   JOIN public.bot_send_attempts a ON a.bot_id=e.bot_id AND a.attempt_id=e.attempt_id
   JOIN public.bot_send_intents s ON s.bot_id=a.bot_id AND s.intent_id=a.intent_id
   JOIN public.bot_memory_source_occurrences o ON o.bot_id=i.bot_id AND o.event_id=i.event_id
  WHERE e.bot_id=NEW.bot_id AND e.event_id=NEW.event_id AND a.attempt_id=NEW.attempt_id
   AND e.intent_id=a.intent_id
   AND a.wire_payload_id=NEW.parent_payload_id AND a.state='SENT'
   AND o.source_unit_id=NEW.dependent_source_unit_id
   AND i.author->>'is_self'='true' AND i.author->>'external_user_id'=b.self_id
   AND a.receipt->>'message_id'=i.platform_message_id AND e.platform_message_id=i.platform_message_id
   AND s.binding_id=c.binding_id AND s.conversation_id=c.conversation_id
   AND s.reply_route->>'self_id'=b.self_id AND s.reply_route->>'platform'=c.platform
   AND s.reply_route->>'channel_kind'=c.channel_kind AND s.reply_route->>'channel_id'=c.channel_id
   AND COALESCE(s.reply_route->>'thread_id','')=c.thread_id
   AND ((e.origin='chat_output' AND s.send_lane='CHAT' AND EXISTS(
     SELECT 1 FROM public.bot_conversation_replies r WHERE r.bot_id=e.bot_id AND r.reply_id=e.reply_id
      AND r.delivery->>'intent_id'=e.intent_id))
    OR (e.origin='control_output' AND s.send_lane='CONTROL' AND EXISTS(
     SELECT 1 FROM public.bot_shell_requests q WHERE q.bot_id=e.bot_id AND q.command_id=e.command_id
      AND q.command_id=s.command_id AND q.status='COMPLETED' AND q.delivery->>'intent_id'=e.intent_id)))
 ) THEN RAISE EXCEPTION 'SOURCE_LINK_PROOF_INVALID' USING ERRCODE='55000'; END IF;
 -- Discovery must preserve a valid proof even when adding it exceeds a later
 -- consumer's read budget. UNION deduplicates cyclic or repeated references;
 -- bounded reads fail closed, while suppression propagates through flat heads.
 RETURN NEW;
END $$;
CREATE TRIGGER bot_source_link_proof BEFORE INSERT ON public.bot_payload_source_links
 FOR EACH ROW EXECUTE FUNCTION public.forest_source_link_guard();

CREATE FUNCTION public.forest_revoke_source_link(selected_bot text,selected_event_id text)
RETURNS boolean LANGUAGE plpgsql AS $$
DECLARE current_epoch bigint; cause text; targets jsonb;
        new_operation text; detail jsonb; link public.bot_payload_source_links;
BEGIN
 SELECT deletion_epoch INTO current_epoch FROM public.bot_memory_clock WHERE bot_id=selected_bot FOR UPDATE;
 SELECT * INTO link FROM public.bot_payload_source_links WHERE bot_id=selected_bot AND event_id=selected_event_id;
 IF link.bot_id IS NULL THEN RETURN false; END IF;
 SELECT forget_operation_id INTO cause FROM public.bot_payload_headers
  WHERE bot_id=selected_bot AND payload_id=link.parent_payload_id AND state IN ('REVOKED','SCRUBBED');
 IF cause IS NULL OR EXISTS(SELECT 1 FROM public.bot_payload_suppression x
  WHERE x.bot_id=selected_bot AND x.source_unit_id=link.dependent_source_unit_id AND x.part_id='*') THEN
  RETURN false;
 END IF;
 -- Each durable decision is deliberately one source. Updating all of its flat
 -- copies exposes revoked parent heads for the next bounded propagation batch.
 -- No real link or existing deletion decision is dropped on a graph-size limit.
 targets := jsonb_build_array(jsonb_build_object('source_unit_id',link.dependent_source_unit_id,'part_id','*'));
 new_operation := 'echo-forget:' || link.link_id;
 INSERT INTO public.bot_payload_forget_requests(bot_id,operation_id,input_digest,targets,deletion_epoch)
 VALUES (selected_bot,new_operation,'sha256:' || encode(sha256(convert_to(
  'forest.echo.forget.v1:' || link.link_id || ':' || cause,'UTF8')),'hex'),targets,current_epoch+1);
 INSERT INTO public.bot_payload_suppression(bot_id,source_unit_id,part_id,operation_id,deletion_epoch)
 VALUES(selected_bot,link.dependent_source_unit_id,'*',new_operation,current_epoch+1);
 UPDATE public.bot_memory_clock SET deletion_epoch=current_epoch+1,fact_revision=fact_revision+1 WHERE bot_id=selected_bot;
 UPDATE public.bot_payload_headers h SET state='REVOKED',forget_operation_id=new_operation
  WHERE h.bot_id=selected_bot AND h.state='ACTIVE' AND EXISTS(
   SELECT 1 FROM public.bot_payload_sources s WHERE s.bot_id=h.bot_id AND s.payload_id=h.payload_id
    AND s.source_unit_id=link.dependent_source_unit_id);
 detail := jsonb_build_object('link_id',link.link_id,'event_id',link.event_id,
  'source_unit_id',link.dependent_source_unit_id,'cause_operation_id',cause,
  'deletion_epoch',current_epoch+1,'source_count',1);
 INSERT INTO public.bot_audit_records(bot_id,audit_id,action,operation_id,details)
 VALUES(selected_bot,new_operation,'memory.exact_echo_revoked',new_operation,detail);
 INSERT INTO public.bot_outbox_events(bot_id,event_id,topic,operation_id,payload)
 VALUES(selected_bot,new_operation,'memory.exact_echo_revoked',new_operation,detail);
 RETURN true;
END $$;

CREATE FUNCTION public.forest_reconcile_source_links(selected_bot text,maximum integer)
RETURNS integer LANGUAGE plpgsql AS $$
DECLARE candidate record; changed integer := 0;
BEGIN
 IF maximum IS NULL OR maximum NOT BETWEEN 1 AND 256 THEN
  RAISE EXCEPTION 'INVALID_SOURCE_LINK_BATCH' USING ERRCODE='22023';
 END IF;
 PERFORM 1 FROM public.bot_memory_clock WHERE bot_id=selected_bot FOR UPDATE;
 FOR candidate IN SELECT l.event_id FROM public.bot_payload_source_links l
  JOIN public.bot_payload_headers h ON h.bot_id=l.bot_id AND h.payload_id=l.parent_payload_id
  WHERE l.bot_id=selected_bot AND h.state IN ('REVOKED','SCRUBBED') AND NOT EXISTS(
   SELECT 1 FROM public.bot_payload_suppression x WHERE x.bot_id=l.bot_id
    AND x.source_unit_id=l.dependent_source_unit_id AND x.part_id='*')
  ORDER BY l.event_id LIMIT maximum LOOP
  IF public.forest_revoke_source_link(selected_bot,candidate.event_id) THEN changed := changed+1; END IF;
 END LOOP;
 RETURN changed;
END $$;

CREATE FUNCTION public.forest_source_link_revoke() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 UPDATE public.bot_memory_clock SET fact_revision=fact_revision+1 WHERE bot_id=NEW.bot_id;
 PERFORM public.forest_revoke_source_link(NEW.bot_id,NEW.event_id);
 RETURN NEW;
END $$;
CREATE TRIGGER bot_source_link_revoke AFTER INSERT ON public.bot_payload_source_links
 FOR EACH ROW EXECUTE FUNCTION public.forest_source_link_revoke();
