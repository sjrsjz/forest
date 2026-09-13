CREATE TABLE public.bot_memory_index_cursors (
 bot_id text NOT NULL REFERENCES public.forest_bot_identity(bot_id), space_id text NOT NULL,
 cursor_id text NOT NULL DEFAULT '', pending_cursor_id text NOT NULL DEFAULT '', cycles bigint NOT NULL DEFAULT 0,
 observed_fact_revision bigint NOT NULL DEFAULT 0,
 PRIMARY KEY(bot_id,space_id)
);
CREATE TABLE public.bot_memory_index_facets (
 bot_id text NOT NULL, facet_id text NOT NULL, space_id text NOT NULL,
 source_unit_id text NOT NULL, source_revision bigint NOT NULL,
 part_id text NOT NULL, selector jsonb NOT NULL,
 state text NOT NULL CHECK(state IN ('QUEUED','WAIT_MEDIA','PENDING','READY','REJECTED','UNKNOWN','SUPPRESSED')),
 request_id text, vector_payload_id text, error_code text,
 created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
 PRIMARY KEY(bot_id,facet_id),
 FOREIGN KEY(bot_id,source_unit_id) REFERENCES public.bot_memory_source_units(bot_id,source_unit_id),
 FOREIGN KEY(bot_id,vector_payload_id) REFERENCES public.bot_payload_headers(bot_id,payload_id)
);
CREATE INDEX memory_index_pending ON public.bot_memory_index_facets(bot_id,space_id,state,facet_id);
CREATE TABLE public.bot_memory_embedding_requests (
 bot_id text NOT NULL, request_id text NOT NULL, purpose text NOT NULL CHECK(purpose IN ('document','query')),
 config_revision bigint NOT NULL, space_id text NOT NULL, facet_ids jsonb NOT NULL,
 conversation_id text, input_cut bigint, deletion_epoch bigint NOT NULL,
 body_payload_id text NOT NULL, manifest_payload_id text NOT NULL, wire_digest text NOT NULL,
 profile jsonb NOT NULL, runtime_fence jsonb NOT NULL, created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
 PRIMARY KEY(bot_id,request_id),
 FOREIGN KEY(bot_id,config_revision) REFERENCES public.bot_memory_config_revisions(bot_id,revision),
 FOREIGN KEY(bot_id,body_payload_id) REFERENCES public.bot_payload_headers(bot_id,payload_id),
 FOREIGN KEY(bot_id,manifest_payload_id) REFERENCES public.bot_payload_headers(bot_id,payload_id),
 CHECK ((purpose='query')=(conversation_id IS NOT NULL AND input_cut IS NOT NULL))
);
CREATE TABLE public.bot_memory_embedding_attempts (
 bot_id text NOT NULL, attempt_id text NOT NULL, request_id text NOT NULL,
 ordinal integer NOT NULL CHECK(ordinal BETWEEN 1 AND 3),
 host_incarnation text NOT NULL, host_owner_epoch bigint NOT NULL,
 state text NOT NULL CHECK(state IN ('PREPARED','DISPATCHING','RESPONDED','NOT_SENT','REJECTED','UNKNOWN','CANCELLED')),
 budget_day date NOT NULL, reserved_tokens bigint NOT NULL CHECK(reserved_tokens>0),
 charged_tokens bigint CHECK(charged_tokens>=0), result_payload_id text, result_digest text, dispatch_token text, error_code text,
 deadline_at timestamptz NOT NULL, created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
 PRIMARY KEY(bot_id,attempt_id), UNIQUE(bot_id,request_id,ordinal),
 FOREIGN KEY(bot_id,request_id) REFERENCES public.bot_memory_embedding_requests(bot_id,request_id),
 FOREIGN KEY(bot_id,result_payload_id) REFERENCES public.bot_payload_headers(bot_id,payload_id),
 CHECK(dispatch_token IS NULL OR length(dispatch_token) BETWEEN 1 AND 128),
 CHECK(state<>'DISPATCHING' OR dispatch_token IS NOT NULL),
 CHECK(state<>'RESPONDED' OR (dispatch_token IS NOT NULL AND charged_tokens IS NOT NULL AND result_digest IS NOT NULL)),
 CHECK(state<>'NOT_SENT' OR charged_tokens IS NULL OR charged_tokens=0),
 CHECK(state NOT IN ('PREPARED','DISPATCHING') OR (charged_tokens IS NULL AND result_payload_id IS NULL AND result_digest IS NULL))
);
CREATE INDEX memory_embedding_recovery ON public.bot_memory_embedding_attempts(bot_id,state,deadline_at);
CREATE TABLE public.bot_memory_embedding_budget_days (
 bot_id text NOT NULL REFERENCES public.forest_bot_identity(bot_id), budget_day date NOT NULL,
 reserved_tokens bigint NOT NULL DEFAULT 0 CHECK(reserved_tokens>=0),
 charged_tokens bigint NOT NULL DEFAULT 0 CHECK(charged_tokens>=0),
 requests bigint NOT NULL DEFAULT 0 CHECK(requests>=0),
 PRIMARY KEY(bot_id,budget_day)
);
-- This replaceable exact-search projection is tied to the removable vector body.
-- Source revocation hides it immediately; physical body scrubbing cascades here.
CREATE TABLE public.bot_memory_vectors (
 bot_id text NOT NULL, facet_id text NOT NULL, space_id text NOT NULL,
 source_unit_id text NOT NULL, payload_id text NOT NULL, vector real[] NOT NULL,
 PRIMARY KEY(bot_id,facet_id),
 FOREIGN KEY(bot_id,facet_id) REFERENCES public.bot_memory_index_facets(bot_id,facet_id),
 FOREIGN KEY(bot_id,payload_id) REFERENCES public.bot_payload_bodies(bot_id,payload_id) ON DELETE CASCADE,
 CHECK(cardinality(vector) BETWEEN 1 AND 1024)
);
CREATE INDEX memory_vector_space ON public.bot_memory_vectors(bot_id,space_id,facet_id);
CREATE FUNCTION public.forest_memory_index_fixed() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 IF TG_TABLE_NAME='bot_memory_index_facets' THEN
  IF (to_jsonb(NEW)-ARRAY['state','request_id','vector_payload_id','error_code'])
     IS DISTINCT FROM (to_jsonb(OLD)-ARRAY['state','request_id','vector_payload_id','error_code']) THEN
   RAISE EXCEPTION 'memory facet identity is immutable';
  END IF;
 ELSE
  RAISE EXCEPTION 'memory embedding request is immutable';
 END IF;
 RETURN NEW;
END $$;
CREATE TRIGGER memory_index_facet_fixed BEFORE UPDATE ON public.bot_memory_index_facets
 FOR EACH ROW EXECUTE FUNCTION public.forest_memory_index_fixed();
CREATE TRIGGER memory_embedding_request_fixed BEFORE UPDATE OR DELETE ON public.bot_memory_embedding_requests
 FOR EACH ROW EXECUTE FUNCTION public.forest_memory_index_fixed();

-- Transport and budget identity survive source scrubbing and configuration
-- changes. A deadline can be shortened to revoke authority, never extended.
-- Recovery may finalize an attempt without an observation digest; that terminal
-- receipt still cannot later become permission to dispatch or settle twice.
CREATE FUNCTION public.forest_memory_embedding_attempt_fixed() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 IF TG_OP='DELETE' THEN
  RAISE EXCEPTION 'memory embedding attempt receipt is immutable' USING ERRCODE='55000';
 END IF;
 IF TG_OP='INSERT' THEN
  IF NEW.state<>'PREPARED' OR NEW.dispatch_token IS NOT NULL
     OR NEW.charged_tokens IS NOT NULL OR NEW.result_digest IS NOT NULL OR NEW.result_payload_id IS NOT NULL THEN
   RAISE EXCEPTION 'memory embedding attempt must begin with an undispatched reservation' USING ERRCODE='55000';
  END IF;
  RETURN NEW;
 END IF;
 IF (to_jsonb(NEW)-ARRAY['state','charged_tokens','result_payload_id','result_digest','dispatch_token','error_code','deadline_at'])
    IS DISTINCT FROM (to_jsonb(OLD)-ARRAY['state','charged_tokens','result_payload_id','result_digest','dispatch_token','error_code','deadline_at'])
    OR NEW.deadline_at>OLD.deadline_at THEN
  RAISE EXCEPTION 'memory embedding attempt identity is immutable' USING ERRCODE='55000';
 END IF;
 IF OLD.state NOT IN ('PREPARED','DISPATCHING') THEN
  IF NEW IS DISTINCT FROM OLD THEN
   RAISE EXCEPTION 'memory embedding terminal observation is immutable' USING ERRCODE='55000';
  END IF;
  RETURN NEW;
 END IF;
 IF OLD.dispatch_token IS NOT NULL AND NEW.dispatch_token IS DISTINCT FROM OLD.dispatch_token THEN
  RAISE EXCEPTION 'memory embedding dispatch owner is immutable' USING ERRCODE='55000';
 END IF;
 IF OLD.state='PREPARED' THEN
  IF NEW.state NOT IN ('PREPARED','DISPATCHING','NOT_SENT','CANCELLED')
     OR (NEW.state<>'DISPATCHING' AND NEW.dispatch_token IS NOT NULL) THEN
   RAISE EXCEPTION 'memory embedding response requires a committed dispatch' USING ERRCODE='55000';
  END IF;
 ELSIF NEW.state NOT IN ('DISPATCHING','RESPONDED','NOT_SENT','REJECTED','UNKNOWN','CANCELLED') THEN
  RAISE EXCEPTION 'memory embedding dispatch cannot return to a reservation' USING ERRCODE='55000';
 END IF;
 RETURN NEW;
END $$;
CREATE TRIGGER memory_embedding_attempt_fixed BEFORE INSERT OR UPDATE OR DELETE ON public.bot_memory_embedding_attempts
 FOR EACH ROW EXECUTE FUNCTION public.forest_memory_embedding_attempt_fixed();
