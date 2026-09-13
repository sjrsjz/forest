-- Prepare control operation results. The frozen Python handler imports only
-- existing results; old request digests are not reconstructed request bodies.
DROP TRIGGER bot_web_operations_immutable ON public.bot_web_operations;
ALTER TABLE public.bot_web_operations
 ALTER COLUMN result DROP NOT NULL,
 ADD COLUMN input_payload_id text,
 ADD COLUMN input_payload_digest text,
 ADD COLUMN input_legacy_unknown boolean NOT NULL DEFAULT true,
 ADD COLUMN result_payload_id text,
 ADD COLUMN result_payload_digest text,
 ADD FOREIGN KEY(bot_id,input_payload_id) REFERENCES public.bot_payload_headers(bot_id,payload_id),
 ADD FOREIGN KEY(bot_id,result_payload_id) REFERENCES public.bot_payload_headers(bot_id,payload_id);
