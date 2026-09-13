-- Component values and local Watch specifications are removable bodies.
-- Immutable package-cache artifact paths remain package location facts.
DROP TRIGGER bot_config_plans_immutable ON public.bot_config_plans;
DROP TRIGGER bot_config_revisions_immutable ON public.bot_config_revisions;
ALTER TABLE public.bot_config_plans ALTER COLUMN values DROP NOT NULL,
 ADD COLUMN values_payload_id text, ADD COLUMN values_payload_digest text,
 ADD COLUMN values_metadata jsonb,
 ADD FOREIGN KEY(bot_id,values_payload_id) REFERENCES public.bot_payload_headers(bot_id,payload_id);
ALTER TABLE public.bot_config_revisions ALTER COLUMN values DROP NOT NULL,
 ADD COLUMN values_payload_id text, ADD COLUMN values_payload_digest text,
 ADD COLUMN values_metadata jsonb,
 ADD FOREIGN KEY(bot_id,values_payload_id) REFERENCES public.bot_payload_headers(bot_id,payload_id);
ALTER TABLE public.bot_dev_watch_heads
 ALTER COLUMN source_identity DROP NOT NULL, ALTER COLUMN config DROP NOT NULL, ALTER COLUMN grants DROP NOT NULL,
 ADD COLUMN spec_payload_id text, ADD COLUMN spec_payload_digest text,
 ADD FOREIGN KEY(bot_id,spec_payload_id) REFERENCES public.bot_payload_headers(bot_id,payload_id);

CREATE FUNCTION public.forest_component_values_metadata(value jsonb) RETURNS jsonb
LANGUAGE plpgsql IMMUTABLE AS $$
BEGIN
 IF value IS NULL OR jsonb_typeof(value) IS DISTINCT FROM 'object'
  OR NOT(value ?& ARRAY['package_digest','config','grants'])
  OR value-ARRAY['package_digest','config','grants']<>'{}'::jsonb
  OR jsonb_typeof(value->'package_digest') IS DISTINCT FROM 'string'
  OR length(value->>'package_digest') NOT BETWEEN 1 AND 256
  OR jsonb_typeof(value->'config') IS DISTINCT FROM 'object'
  OR jsonb_typeof(value->'grants') IS DISTINCT FROM 'array' THEN
  RAISE EXCEPTION 'CONFIGURATION_VALUES_INVALID' USING ERRCODE='23514';
 END IF;
 IF jsonb_array_length(value->'grants')>256 OR EXISTS(
  SELECT 1 FROM jsonb_array_elements(value->'grants') g
  WHERE jsonb_typeof(g) IS DISTINCT FROM 'string' OR length(g#>>'{}') NOT BETWEEN 1 AND 128)
  OR (SELECT count(*)<>count(DISTINCT g) FROM jsonb_array_elements(value->'grants') g) THEN
  RAISE EXCEPTION 'CONFIGURATION_VALUES_INVALID' USING ERRCODE='23514';
 END IF;
 RETURN jsonb_build_object('package_digest',value->'package_digest','grants',value->'grants');
END $$;

CREATE FUNCTION public.forest_component_metadata_valid(value jsonb) RETURNS boolean
LANGUAGE plpgsql IMMUTABLE AS $$
BEGIN
 IF value IS NULL OR jsonb_typeof(value) IS DISTINCT FROM 'object'
  OR NOT(value ?& ARRAY['package_digest','grants']) OR value-ARRAY['package_digest','grants']<>'{}'::jsonb THEN RETURN false; END IF;
 RETURN public.forest_component_values_metadata(value||'{"config":{}}'::jsonb)=value;
EXCEPTION WHEN check_violation THEN RETURN false;
END $$;
