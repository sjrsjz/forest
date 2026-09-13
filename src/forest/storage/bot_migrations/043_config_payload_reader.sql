-- Host callers acquire their authority/business locks before this reader's
-- memory clock. Reading content neither admits an operation nor looks up an
-- owner business row: values precede immutable plans/revisions during INSERT.
CREATE FUNCTION public.forest_config_payload_values(
 selected_bot text, selected_payload text, selected_digest text,
 selected_family text, selected_kind text, selected_resource text, selected_identity text)
RETURNS jsonb LANGUAGE plpgsql AS $$
DECLARE
 h record; parent record; envelope jsonb; actual_parents jsonb; actual_sources jsonb;
 expected_owner text; expected_payload text; identity_json text; source_count bigint;
 suppressed boolean; clock_exists boolean;
BEGIN
 IF selected_family IS NULL OR selected_family NOT IN
      ('config','agent.config','memory.config','binding','watch')
  OR selected_kind IS NULL OR selected_kind NOT IN ('PLAN','REVISION','INITIAL')
  OR selected_resource IS NULL OR octet_length(selected_resource)>4096
  OR selected_identity IS NULL OR selected_identity='' OR octet_length(selected_identity)>4096
  OR selected_resource ~ '[\x01-\x1f\x7f]' OR selected_identity ~ '[\x01-\x1f\x7f]'
  OR (selected_kind='REVISION' AND selected_identity !~ '^(0|[1-9][0-9]*)$')
  OR selected_digest IS NULL OR length(selected_digest)<>71
  OR selected_digest !~ '^sha256:[a-f0-9]{64}$'
  OR selected_bot IS NULL OR selected_payload IS NULL THEN
  RAISE EXCEPTION 'CONFIG_PAYLOAD_IDENTITY_CONFLICT' USING ERRCODE='23514';
 END IF;

 -- jsonb array rendering inserts spaces. Serialize each string independently
 -- and join with literal commas to match Python's compact UTF-8 digest_json;
 -- never strip spaces inside an identity or change its Unicode spelling.
 identity_json := '[' || to_jsonb(selected_family)::text || ',' || to_jsonb(selected_kind)::text
  || ',' || to_jsonb(selected_resource)::text || ',' || to_jsonb(selected_identity)::text || ']';
 expected_owner := 'config-values-v1:sha256:' || encode(sha256(convert_to(identity_json,'UTF8')),'hex');
 expected_payload := 'payload:sha256:' || encode(sha256(convert_to(
  '[' || to_jsonb(selected_bot)::text || ',"control.config",' || to_jsonb(expected_owner)::text || ']',
  'UTF8')),'hex');
 IF selected_payload IS DISTINCT FROM expected_payload THEN
  RAISE EXCEPTION 'CONFIG_PAYLOAD_IDENTITY_CONFLICT' USING ERRCODE='23514';
 END IF;

 PERFORM 1 FROM public.bot_memory_clock WHERE bot_id=selected_bot FOR UPDATE;
 clock_exists := FOUND;
 SELECT p.*,b.content INTO h FROM public.bot_payload_headers p
  LEFT JOIN public.bot_payload_bodies b USING(bot_id,payload_id)
  WHERE p.bot_id=selected_bot AND p.payload_id=selected_payload;
 IF NOT FOUND OR h.owner_kind IS DISTINCT FROM 'control.config'
  OR h.owner_id IS DISTINCT FROM expected_owner OR h.encoding IS DISTINCT FROM 'json'
  OR h.digest IS DISTINCT FROM selected_digest
  OR jsonb_typeof(h.manifest) IS DISTINCT FROM 'object'
  OR NOT (h.manifest ?& ARRAY['parents','sources'])
  OR h.manifest-ARRAY['parents','sources'] <> '{}'::jsonb
  OR jsonb_typeof(h.manifest->'parents') IS DISTINCT FROM 'array'
  OR jsonb_typeof(h.manifest->'sources') IS DISTINCT FROM 'array' THEN
  RAISE EXCEPTION 'CONFIG_PAYLOAD_IDENTITY_CONFLICT' USING ERRCODE='23514';
 END IF;
 IF jsonb_array_length(h.manifest->'parents') NOT BETWEEN 1 AND 256
  OR jsonb_array_length(h.manifest->'sources')>4096 THEN
  RAISE EXCEPTION 'CONFIG_PAYLOAD_IDENTITY_CONFLICT' USING ERRCODE='23514';
 END IF;
 SELECT COALESCE(jsonb_agg(p.parent_payload_id ORDER BY p.parent_payload_id COLLATE "C"),'[]'::jsonb)
  INTO actual_parents FROM (
   SELECT parent_payload_id FROM public.bot_payload_derivations
    WHERE bot_id=selected_bot AND payload_id=selected_payload
    ORDER BY parent_payload_id COLLATE "C" LIMIT 257
  ) p;
 -- Equality also rejects duplicate, unordered, non-string and fictitious refs.
 IF actual_parents IS DISTINCT FROM h.manifest->'parents' THEN
  RAISE EXCEPTION 'CONFIG_PAYLOAD_IDENTITY_CONFLICT' USING ERRCODE='23514';
 END IF;
 IF EXISTS(SELECT 1 FROM jsonb_array_elements_text(actual_parents) p(id)
  WHERE p.id='' OR octet_length(p.id)>256 OR p.id ~ '[\x01-\x1f\x7f]') THEN
  RAISE EXCEPTION 'CONFIG_PAYLOAD_IDENTITY_CONFLICT' USING ERRCODE='23514';
 END IF;
 SELECT COALESCE(jsonb_agg(jsonb_build_object(
  'source_unit_id',s.source_unit_id,'source_revision',s.source_revision,'part_id',s.part_id)), '[]'::jsonb)
  INTO actual_sources FROM public.bot_payload_sources s
  WHERE s.bot_id=selected_bot AND s.payload_id=selected_payload;
 IF jsonb_array_length(actual_sources)<>jsonb_array_length(h.manifest->'sources')
  OR NOT (actual_sources @> (h.manifest->'sources') AND (h.manifest->'sources') @> actual_sources) THEN
  RAISE EXCEPTION 'CONFIG_PAYLOAD_IDENTITY_CONFLICT' USING ERRCODE='23514';
 END IF;
 IF NOT clock_exists OR h.state<>'ACTIVE' OR h.content IS NULL THEN
  RAISE EXCEPTION 'CONTROL_CONTENT_REVOKED' USING ERRCODE='P0001';
 END IF;

 -- Flattened sources alone do not establish parent body availability. A
 -- revoked/incomplete parent cannot be used even while this child's bytes exist.
 FOR parent IN SELECT p.state,p.manifest,b.payload_id AS body_id
  FROM public.bot_payload_derivations d
  JOIN public.bot_payload_headers p ON p.bot_id=d.bot_id AND p.payload_id=d.parent_payload_id
  LEFT JOIN public.bot_payload_bodies b ON b.bot_id=p.bot_id AND b.payload_id=p.payload_id
  WHERE d.bot_id=selected_bot AND d.payload_id=selected_payload
 LOOP
  IF parent.state<>'ACTIVE' OR parent.body_id IS NULL THEN
   RAISE EXCEPTION 'CONTROL_CONTENT_REVOKED' USING ERRCODE='P0001';
  END IF;
  IF jsonb_typeof(parent.manifest->'sources') IS DISTINCT FROM 'array'
   OR NOT (actual_sources @> (parent.manifest->'sources')) THEN
   RAISE EXCEPTION 'CONFIG_PAYLOAD_IDENTITY_CONFLICT' USING ERRCODE='23514';
  END IF;
 END LOOP;
 -- All direct parent sources are proven included above. Apply the current
 -- exact-echo closure to the complete union; 4097 is an error, never a truncation.
 SELECT count(*),COALESCE(bool_or(EXISTS(
  SELECT 1 FROM public.bot_payload_suppression s WHERE s.bot_id=selected_bot
   AND s.source_unit_id=d.source_unit_id
   AND (s.part_id='*' OR d.part_id='*' OR s.part_id=d.part_id))),false)
  INTO source_count,suppressed
  FROM public.forest_source_dependency_closure(selected_bot,actual_sources,4096) d;
 IF source_count>4096 OR suppressed THEN
  RAISE EXCEPTION 'CONTROL_CONTENT_REVOKED' USING ERRCODE='P0001';
 END IF;
 IF h.byte_length IS DISTINCT FROM octet_length(h.content)::bigint
  OR h.digest IS DISTINCT FROM ('sha256:' || encode(sha256(h.content),'hex')) THEN
  RAISE EXCEPTION 'CONFIG_PAYLOAD_IDENTITY_CONFLICT' USING ERRCODE='23514';
 END IF;
 BEGIN
  envelope := convert_from(h.content,'UTF8')::jsonb;
 EXCEPTION WHEN invalid_text_representation OR character_not_in_repertoire OR untranslatable_character THEN
  RAISE EXCEPTION 'CONFIG_PAYLOAD_IDENTITY_CONFLICT' USING ERRCODE='23514';
 END;
 IF jsonb_typeof(envelope) IS DISTINCT FROM 'object'
  OR NOT (envelope ?& ARRAY['family','kind','resource','identity','values'])
  OR envelope-ARRAY['family','kind','resource','identity','values']<>'{}'::jsonb
  OR envelope->'family' IS DISTINCT FROM to_jsonb(selected_family)
  OR envelope->'kind' IS DISTINCT FROM to_jsonb(selected_kind)
  OR envelope->'resource' IS DISTINCT FROM to_jsonb(selected_resource)
  OR envelope->'identity' IS DISTINCT FROM to_jsonb(selected_identity) THEN
  RAISE EXCEPTION 'CONFIG_PAYLOAD_IDENTITY_CONFLICT' USING ERRCODE='23514';
 END IF;
 RETURN envelope->'values';
END;
$$;
