-- A Bot selects one native multimodal embedding space at initial memory setup.
-- Endpoint, credentials and budgets may rotate; model identity cannot be rebound.
DO $$
BEGIN
 IF NOT EXISTS(SELECT 1 FROM public.bot_schema_writer_gate
               WHERE singleton AND minimum_generation>=26) THEN
  RAISE EXCEPTION 'WRITER_GATE_NOT_INSTALLED' USING ERRCODE='55000';
 END IF;
END;
$$;

CREATE OR REPLACE FUNCTION public.forest_memory_config_metadata_valid(value jsonb)
RETURNS boolean LANGUAGE plpgsql IMMUTABLE AS $$
DECLARE rule record; item numeric;
BEGIN
 IF value IS NULL OR jsonb_typeof(value)<>'object'
  OR NOT(value ?& ARRAY['enabled','profile_deployment_digest','profile_model_digest',
   'profile_revision_digest','dimensions','batch_size','index_concurrency','request_timeout',
   'daily_request_budget','daily_token_budget','request_token_reservation','capture_concurrency',
   'capture_timeout','capture_daily_bytes','capture_storage_bytes','media_max_bytes','capture_max_attempts',
   'query_max_parts','query_max_images','query_limit','query_timeout','max_result_tokens','max_result_images','max_result_bytes'])
  OR value-ARRAY['enabled','profile_deployment_digest','profile_model_digest',
   'profile_revision_digest','dimensions','batch_size','index_concurrency','request_timeout',
   'daily_request_budget','daily_token_budget','request_token_reservation','capture_concurrency',
   'capture_timeout','capture_daily_bytes','capture_storage_bytes','media_max_bytes','capture_max_attempts',
   'query_max_parts','query_max_images','query_limit','query_timeout','max_result_tokens','max_result_images','max_result_bytes']<>'{}'::jsonb
  OR jsonb_typeof(value->'enabled') IS DISTINCT FROM 'boolean' THEN RETURN false; END IF;
 FOR rule IN SELECT * FROM (VALUES
  ('dimensions',64::numeric,4096::numeric),('batch_size',1,32),('index_concurrency',1,4),
  ('request_timeout',1,120),('daily_request_budget',1,100000),('daily_token_budget',1,1000000000),
  ('request_token_reservation',1,1000000),('capture_concurrency',1,4),('capture_timeout',1,30),
  ('capture_daily_bytes',1,9007199254740991),('capture_storage_bytes',1,9007199254740991),
  ('media_max_bytes',1,4194304),('capture_max_attempts',1,10),('query_max_parts',1,32),
  ('query_max_images',0,8),('query_limit',1,32),('query_timeout',1,60),
  ('max_result_tokens',256,16000),('max_result_images',0,8),('max_result_bytes',1,33554432)
 ) r(key,minimum,maximum) LOOP
  IF jsonb_typeof(value->rule.key) IS DISTINCT FROM 'number' THEN RETURN false; END IF;
  item := (value->>rule.key)::numeric;
  IF item<>trunc(item) OR item<rule.minimum OR item>rule.maximum THEN RETURN false; END IF;
 END LOOP;
 RETURN (value->>'dimensions')::numeric IN (64,128,256,512,768,1024,2048,4096)
  AND COALESCE(value->>'profile_deployment_digest' ~ '^sha256:[0-9a-f]{64}$',false)
  AND COALESCE(value->>'profile_model_digest' ~ '^sha256:[0-9a-f]{64}$',false)
  AND COALESCE(value->>'profile_revision_digest' ~ '^sha256:[0-9a-f]{64}$',false);
END $$;

CREATE OR REPLACE FUNCTION public.forest_memory_config_metadata(value jsonb)
RETURNS jsonb LANGUAGE plpgsql IMMUTABLE AS $$
DECLARE result jsonb;
BEGIN
 SELECT jsonb_object_agg(key,value->key) INTO result FROM unnest(ARRAY[
  'enabled','dimensions','batch_size','index_concurrency','request_timeout','daily_request_budget',
  'daily_token_budget','request_token_reservation','capture_concurrency','capture_timeout',
  'capture_daily_bytes','capture_storage_bytes','media_max_bytes','capture_max_attempts',
  'query_max_parts','query_max_images','query_limit','query_timeout','max_result_tokens','max_result_images','max_result_bytes'
 ]) key;
 result := result || jsonb_build_object(
  'profile_deployment_digest',public.forest_memory_identity_digest(value->>'deployment_id'),
  'profile_model_digest',public.forest_memory_identity_digest(value->>'model'),
  'profile_revision_digest',public.forest_memory_identity_digest(value->>'model_revision'));
 IF NOT public.forest_memory_config_metadata_valid(result) THEN
  RAISE EXCEPTION 'MEMORY_CONFIG_METADATA_INVALID' USING ERRCODE='23514';
 END IF;
 RETURN result;
END $$;

-- Preserve lifecycle identity as non-content digests before a later source erasure.
ALTER TABLE public.bot_memory_config_plans DISABLE TRIGGER bot_memory_config_plans_immutable;
ALTER TABLE public.bot_memory_config_revisions DISABLE TRIGGER bot_memory_config_revisions_immutable;
UPDATE public.bot_memory_config_plans p SET values_metadata=public.forest_memory_config_metadata(
 public.forest_config_payload_values(p.bot_id,p.values_payload_id,p.values_payload_digest,
  'memory.config','PLAN','',p.plan_id))
WHERE EXISTS(SELECT 1 FROM public.bot_payload_bodies b
             WHERE (b.bot_id,b.payload_id)=(p.bot_id,p.values_payload_id));
UPDATE public.bot_memory_config_revisions r SET values_metadata=public.forest_memory_config_metadata(
 public.forest_config_payload_values(r.bot_id,r.values_payload_id,r.values_payload_digest,
  'memory.config','REVISION','',r.revision::text))
WHERE EXISTS(SELECT 1 FROM public.bot_payload_bodies b
             WHERE (b.bot_id,b.payload_id)=(r.bot_id,r.values_payload_id));
ALTER TABLE public.bot_memory_config_plans ENABLE TRIGGER bot_memory_config_plans_immutable;
ALTER TABLE public.bot_memory_config_revisions ENABLE TRIGGER bot_memory_config_revisions_immutable;

DO $$
DECLARE item record;
BEGIN
 FOR item IN SELECT conname FROM pg_constraint
  WHERE conrelid='public.bot_memory_embedding_requests'::regclass
   AND contype='c' AND pg_get_constraintdef(oid) LIKE '%profile_metadata%'
 LOOP
  EXECUTE format('ALTER TABLE public.bot_memory_embedding_requests DROP CONSTRAINT %I',item.conname);
 END LOOP;
 FOR item IN SELECT conname FROM pg_constraint
  WHERE conrelid='public.bot_memory_vectors'::regclass
   AND contype='c' AND pg_get_constraintdef(oid) LIKE '%cardinality(vector)%'
 LOOP
  EXECUTE format('ALTER TABLE public.bot_memory_vectors DROP CONSTRAINT %I',item.conname);
 END LOOP;
END;
$$;
ALTER TABLE public.bot_memory_embedding_requests
 ADD CONSTRAINT memory_embedding_profile_metadata_v2 CHECK(
  jsonb_typeof(profile_metadata)='object'
  AND profile_metadata ?& ARRAY['dimensions','protocol_revision']
  AND profile_metadata-ARRAY['dimensions','protocol_revision']='{}'::jsonb
  AND (
   (profile_metadata->'dimensions' IN ('512'::jsonb,'1024'::jsonb)
    AND profile_metadata->'protocol_revision'='"jina-clip-v2-http-2026-09-v1"'::jsonb)
   OR
   (profile_metadata->'dimensions' IN
     ('64'::jsonb,'128'::jsonb,'256'::jsonb,'512'::jsonb,'768'::jsonb,
      '1024'::jsonb,'2048'::jsonb,'4096'::jsonb)
    AND profile_metadata->'protocol_revision'=
     '"siliconflow-qwen3-vl-embedding-http-2026-09-v1"'::jsonb)
  ));
ALTER TABLE public.bot_memory_vectors
 ADD CONSTRAINT memory_vector_dimensions_v2 CHECK(cardinality(vector) BETWEEN 1 AND 4096);

CREATE OR REPLACE FUNCTION public.forest_memory_deployment_register() RETURNS trigger
LANGUAGE plpgsql AS $$
DECLARE value jsonb; prior_metadata jsonb; current_revision bigint;
 deployment text; endpoint text;
BEGIN
 value := public.forest_config_payload_values(NEW.bot_id,NEW.values_payload_id,NEW.values_payload_digest,
  'memory.config','REVISION','',NEW.revision::text);
 SELECT revision INTO current_revision FROM public.bot_memory_config_heads WHERE bot_id=NEW.bot_id;
 IF current_revision>0 THEN
  SELECT r.values_metadata INTO prior_metadata FROM public.bot_memory_config_revisions r
  WHERE (r.bot_id,r.revision)=(NEW.bot_id,current_revision);
  IF jsonb_build_array(prior_metadata->'profile_deployment_digest',
                       prior_metadata->'profile_model_digest',
                       prior_metadata->'profile_revision_digest',prior_metadata->'dimensions')
     IS DISTINCT FROM
     jsonb_build_array(NEW.values_metadata->'profile_deployment_digest',
                       NEW.values_metadata->'profile_model_digest',
                       NEW.values_metadata->'profile_revision_digest',NEW.values_metadata->'dimensions') THEN
   RAISE EXCEPTION 'MEMORY_PROFILE_LIFECYCLE_CONFLICT' USING ERRCODE='23514';
  END IF;
 END IF;
 deployment := public.forest_memory_identity_digest(value->>'deployment_id');
 endpoint := public.forest_memory_identity_digest(value->>'endpoint');
 INSERT INTO public.bot_memory_deployment_bindings(bot_id,deployment_digest,endpoint_digest,first_revision)
  VALUES(NEW.bot_id,deployment,endpoint,NEW.revision) ON CONFLICT DO NOTHING;
 RETURN NEW;
END $$;

CREATE OR REPLACE FUNCTION public.forest_memory_request_profile_guard() RETURNS trigger
LANGUAGE plpgsql AS $$
DECLARE r record; h record; value jsonb; expected jsonb; owner text; parent_id text;
 protocol text; query_task jsonb; truncate_value jsonb;
BEGIN
 SELECT * INTO r FROM public.bot_memory_config_revisions WHERE bot_id=NEW.bot_id AND revision=NEW.config_revision;
 IF NOT FOUND THEN RAISE EXCEPTION 'MEMORY_CONFIG_SOURCE_MISSING' USING ERRCODE='23514'; END IF;
 value := public.forest_config_payload_values(NEW.bot_id,r.values_payload_id,r.values_payload_digest,
  'memory.config','REVISION','',r.revision::text);
 owner := 'profile:' || public.forest_memory_identity_digest(NEW.request_id);
 SELECT p.*,b.content INTO h FROM public.bot_payload_headers p LEFT JOIN public.bot_payload_bodies b USING(bot_id,payload_id)
  WHERE p.bot_id=NEW.bot_id AND p.payload_id=NEW.profile_payload_id;
 IF NOT FOUND OR h.owner_kind IS DISTINCT FROM 'memory.embedding' OR h.owner_id IS DISTINCT FROM owner
  OR h.encoding IS DISTINCT FROM 'json' OR h.digest IS DISTINCT FROM NEW.profile_payload_digest
  OR NEW.space_id IS DISTINCT FROM h.digest OR h.state IS DISTINCT FROM 'ACTIVE' OR h.content IS NULL
  OR h.digest IS DISTINCT FROM ('sha256:' || encode(sha256(h.content),'hex')) THEN
  RAISE EXCEPTION 'MEMORY_PROFILE_SOURCE_CONFLICT' USING ERRCODE='23514';
 END IF;
 IF value->>'model'='jina-clip-v2' THEN
  protocol := 'jina-clip-v2-http-2026-09-v1';
  query_task := to_jsonb('retrieval.query'::text);
  truncate_value := 'false'::jsonb;
 ELSIF value->>'model'='Qwen/Qwen3-VL-Embedding-8B' THEN
  protocol := 'siliconflow-qwen3-vl-embedding-http-2026-09-v1';
  query_task := 'null'::jsonb;
  truncate_value := to_jsonb('right'::text);
 ELSE
  RAISE EXCEPTION 'MEMORY_PROFILE_SOURCE_CONFLICT' USING ERRCODE='23514';
 END IF;
 expected := jsonb_build_object('deployment_id',value->'deployment_id','model_revision',value->'model_revision',
  'model',value->'model','dimensions',value->'dimensions','protocol_revision',protocol,
  'document_task',NULL,'query_task',query_task,'normalized',true,'truncate',truncate_value,
  'image_input','authorized_capture_bytes_v1','postprocessing','l2_float32_v1','mixed_strategy','independent_facets_v1');
 IF convert_from(h.content,'UTF8')::jsonb IS DISTINCT FROM expected OR NEW.profile_metadata IS DISTINCT FROM
  jsonb_build_object('dimensions',value->'dimensions','protocol_revision',protocol) THEN
  RAISE EXCEPTION 'MEMORY_PROFILE_SOURCE_CONFLICT' USING ERRCODE='23514';
 END IF;
 FOREACH parent_id IN ARRAY ARRAY[NEW.profile_payload_id,NEW.body_payload_id,NEW.manifest_payload_id] LOOP
  IF NOT EXISTS(SELECT 1 FROM public.bot_payload_derivations WHERE bot_id=NEW.bot_id
   AND payload_id=parent_id AND parent_payload_id=r.values_payload_id) THEN
   RAISE EXCEPTION 'MEMORY_PROFILE_SOURCE_CONFLICT' USING ERRCODE='23514';
  END IF;
 END LOOP;
 RETURN NEW;
END $$;

-- Durable RouteGraph service contracts are part of the unpublished final baseline.
CREATE TABLE public.bot_service_clock (
 bot_id text PRIMARY KEY REFERENCES public.forest_bot_identity(bot_id),
 revision bigint NOT NULL DEFAULT 0 CHECK(revision>=0),
 updated_at timestamptz NOT NULL DEFAULT clock_timestamp()
);

CREATE TABLE public.bot_service_generations (
 bot_id text NOT NULL,
 generation_id text NOT NULL,
 component_id text NOT NULL,
 spec_set_digest text NOT NULL,
 service_count integer NOT NULL CHECK(service_count BETWEEN 0 AND 64),
 requirement_count integer NOT NULL CHECK(requirement_count BETWEEN 0 AND 64),
 created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
 PRIMARY KEY(bot_id,generation_id),
 FOREIGN KEY(bot_id,generation_id) REFERENCES public.bot_generations(bot_id,generation_id)
);

CREATE TABLE public.bot_generation_service_definitions (
 bot_id text NOT NULL,
 generation_id text NOT NULL,
 component_id text NOT NULL,
 service_name text NOT NULL,
 contract_major integer NOT NULL CHECK(contract_major>0),
 contract_minor integer NOT NULL CHECK(contract_minor>=0),
 input_schema_digest text NOT NULL,
 output_schema_digest text NOT NULL,
 PRIMARY KEY(bot_id,generation_id,service_name,contract_major),
 FOREIGN KEY(bot_id,generation_id) REFERENCES public.bot_service_generations(bot_id,generation_id)
);

CREATE TABLE public.bot_generation_service_requirements (
 bot_id text NOT NULL,
 generation_id text NOT NULL,
 component_id text NOT NULL,
 service_name text NOT NULL,
 contract_range text NOT NULL,
 required boolean NOT NULL,
 startup_dependency boolean NOT NULL,
 PRIMARY KEY(bot_id,generation_id,service_name),
 FOREIGN KEY(bot_id,generation_id) REFERENCES public.bot_service_generations(bot_id,generation_id)
);

CREATE TABLE public.bot_service_publications (
 bot_id text NOT NULL REFERENCES public.forest_bot_identity(bot_id),
 registry_revision bigint NOT NULL CHECK(registry_revision>=1),
 component_id text NOT NULL,
 generation_id text,
 route_epoch bigint NOT NULL CHECK(route_epoch>=1),
 operation_kind text NOT NULL CHECK(operation_kind IN ('INSTALL','DISABLE')),
 spec_set_digest text NOT NULL,
 created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
 PRIMARY KEY(bot_id,registry_revision),
 UNIQUE(bot_id,component_id,route_epoch),
 FOREIGN KEY(bot_id,generation_id) REFERENCES public.bot_service_generations(bot_id,generation_id)
);

CREATE TABLE public.bot_service_heads (
 bot_id text NOT NULL REFERENCES public.forest_bot_identity(bot_id),
 service_name text NOT NULL,
 contract_major integer NOT NULL CHECK(contract_major>0),
 component_id text NOT NULL,
 generation_id text,
 route_epoch bigint NOT NULL CHECK(route_epoch>=1),
 registry_revision bigint NOT NULL CHECK(registry_revision>=1),
 contract_minor integer NOT NULL CHECK(contract_minor>=0),
 input_schema_digest text NOT NULL,
 output_schema_digest text NOT NULL,
 active boolean NOT NULL,
 updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
 PRIMARY KEY(bot_id,service_name,contract_major),
 FOREIGN KEY(bot_id,generation_id) REFERENCES public.bot_service_generations(bot_id,generation_id),
 CHECK((active AND generation_id IS NOT NULL) OR (NOT active AND generation_id IS NULL))
);

CREATE FUNCTION public.forest_service_registry_fact_immutable() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
 RAISE EXCEPTION 'SERVICE_REGISTRY_FACT_IMMUTABLE' USING ERRCODE='55000';
END;
$$;
CREATE TRIGGER bot_service_generation_immutable BEFORE UPDATE OR DELETE
 ON public.bot_service_generations FOR EACH ROW EXECUTE FUNCTION public.forest_service_registry_fact_immutable();
CREATE TRIGGER bot_generation_service_definition_immutable BEFORE UPDATE OR DELETE
 ON public.bot_generation_service_definitions FOR EACH ROW EXECUTE FUNCTION public.forest_service_registry_fact_immutable();
CREATE TRIGGER bot_generation_service_requirement_immutable BEFORE UPDATE OR DELETE
 ON public.bot_generation_service_requirements FOR EACH ROW EXECUTE FUNCTION public.forest_service_registry_fact_immutable();
CREATE TRIGGER bot_service_publication_immutable BEFORE UPDATE OR DELETE
 ON public.bot_service_publications FOR EACH ROW EXECUTE FUNCTION public.forest_service_registry_fact_immutable();

CREATE FUNCTION public.forest_service_head_guard() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 IF TG_OP='DELETE' THEN
  RAISE EXCEPTION 'SERVICE_HEAD_DELETE_FORBIDDEN' USING ERRCODE='55000';
 END IF;
 IF ROW(NEW.bot_id,NEW.service_name,NEW.contract_major)
    IS DISTINCT FROM ROW(OLD.bot_id,OLD.service_name,OLD.contract_major) THEN
  RAISE EXCEPTION 'SERVICE_IDENTITY_IMMUTABLE' USING ERRCODE='55000';
 END IF;
 RETURN NEW;
END;
$$;
CREATE TRIGGER bot_service_head_guard BEFORE UPDATE OR DELETE
 ON public.bot_service_heads FOR EACH ROW EXECUTE FUNCTION public.forest_service_head_guard();
