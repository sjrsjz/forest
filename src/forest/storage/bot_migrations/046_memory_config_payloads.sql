-- Values and public embedding profile bodies share the removable payload store.
-- Existing identities, secret ciphertexts and transport/budget facts stay fixed.
DROP TRIGGER bot_memory_config_plans_immutable ON public.bot_memory_config_plans;
DROP TRIGGER bot_memory_config_revisions_immutable ON public.bot_memory_config_revisions;
DROP TRIGGER memory_embedding_request_fixed ON public.bot_memory_embedding_requests;
DROP INDEX public.bot_memory_config_deployment;
ALTER TABLE public.bot_memory_config_plans
 ALTER COLUMN values DROP NOT NULL,
 ADD COLUMN values_payload_id text,
 ADD COLUMN values_payload_digest text,
 ADD COLUMN values_metadata jsonb,
 ADD FOREIGN KEY(bot_id,values_payload_id) REFERENCES public.bot_payload_headers(bot_id,payload_id);
ALTER TABLE public.bot_memory_config_revisions
 ALTER COLUMN values DROP NOT NULL,
 ADD COLUMN values_payload_id text,
 ADD COLUMN values_payload_digest text,
 ADD COLUMN values_metadata jsonb,
 ADD FOREIGN KEY(bot_id,values_payload_id) REFERENCES public.bot_payload_headers(bot_id,payload_id);
ALTER TABLE public.bot_memory_embedding_requests
 ALTER COLUMN profile DROP NOT NULL,
 ADD COLUMN profile_payload_id text,
 ADD COLUMN profile_payload_digest text,
 ADD COLUMN profile_metadata jsonb,
 ADD FOREIGN KEY(bot_id,profile_payload_id) REFERENCES public.bot_payload_headers(bot_id,payload_id);
CREATE TABLE public.bot_memory_deployment_bindings (
 bot_id text NOT NULL,
 deployment_digest text NOT NULL CHECK(deployment_digest ~ '^sha256:[0-9a-f]{64}$'),
 endpoint_digest text NOT NULL CHECK(endpoint_digest ~ '^sha256:[0-9a-f]{64}$'),
 first_revision bigint NOT NULL,
 PRIMARY KEY(bot_id,deployment_digest),
 FOREIGN KEY(bot_id,first_revision) REFERENCES public.bot_memory_config_revisions(bot_id,revision)
);

CREATE FUNCTION public.forest_memory_config_metadata_valid(value jsonb)
RETURNS boolean LANGUAGE plpgsql IMMUTABLE AS $$
DECLARE rule record; item numeric;
BEGIN
 IF value IS NULL OR jsonb_typeof(value)<>'object'
  OR NOT(value ?& ARRAY['enabled','dimensions','batch_size','index_concurrency','request_timeout',
   'daily_request_budget','daily_token_budget','request_token_reservation','capture_concurrency',
   'capture_timeout','capture_daily_bytes','capture_storage_bytes','media_max_bytes','capture_max_attempts',
   'query_max_parts','query_max_images','query_limit','query_timeout','max_result_tokens','max_result_images','max_result_bytes'])
  OR value-ARRAY['enabled','dimensions','batch_size','index_concurrency','request_timeout',
   'daily_request_budget','daily_token_budget','request_token_reservation','capture_concurrency',
   'capture_timeout','capture_daily_bytes','capture_storage_bytes','media_max_bytes','capture_max_attempts',
   'query_max_parts','query_max_images','query_limit','query_timeout','max_result_tokens','max_result_images','max_result_bytes']<>'{}'::jsonb
  OR jsonb_typeof(value->'enabled') IS DISTINCT FROM 'boolean' THEN RETURN false; END IF;
 FOR rule IN SELECT * FROM (VALUES
  ('dimensions',512::numeric,1024::numeric),('batch_size',1,32),('index_concurrency',1,4),
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
 RETURN (value->>'dimensions')::numeric IN (512,1024);
END $$;

CREATE FUNCTION public.forest_memory_config_metadata(value jsonb)
RETURNS jsonb LANGUAGE plpgsql IMMUTABLE AS $$
DECLARE result jsonb;
BEGIN
 SELECT jsonb_object_agg(key,value->key) INTO result FROM unnest(ARRAY[
  'enabled','dimensions','batch_size','index_concurrency','request_timeout','daily_request_budget',
  'daily_token_budget','request_token_reservation','capture_concurrency','capture_timeout',
  'capture_daily_bytes','capture_storage_bytes','media_max_bytes','capture_max_attempts',
  'query_max_parts','query_max_images','query_limit','query_timeout','max_result_tokens','max_result_images','max_result_bytes'
 ]) key;
 IF NOT public.forest_memory_config_metadata_valid(result) THEN
  RAISE EXCEPTION 'MEMORY_CONFIG_METADATA_INVALID' USING ERRCODE='23514';
 END IF;
 RETURN result;
END $$;

-- Hash the exact already-normalized string; keep no URL/deployment text copy.
CREATE FUNCTION public.forest_memory_identity_digest(value text)
RETURNS text LANGUAGE sql IMMUTABLE STRICT AS $$
 SELECT 'sha256:' || encode(sha256(convert_to('[' || to_jsonb(value)::text || ']','UTF8')),'hex')
$$;
