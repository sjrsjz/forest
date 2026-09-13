"""Frozen 041: exact control events and explicit old configuration provenance."""

from forest.storage.migration_v1 import _digest, legacy_source_ref
from forest.storage.payloads import PayloadStore

FAMILIES = (
    ("config", "bot_config_plans", "bot_config_revisions", "component_id"),
    ("agent.config", "bot_agent_config_plans", "bot_agent_config_revisions", None),
    ("memory.config", "bot_memory_config_plans", "bot_memory_config_revisions", None),
    ("binding", "bot_binding_plans", "bot_binding_revisions", "binding_id"),
    ("job", "bot_job_plans", "bot_job_revisions", "job_id"),
)


async def migrate_in(conn, store):
    for table, key, label, body in (
        ("bot_audit_records", "audit_id", "action", "details"),
        ("bot_outbox_events", "event_id", "topic", "payload"),
    ):
        after = None
        while True:
            rows = await (await conn.execute(
                f"SELECT e.*,w.subject_id,w.result_payload_id,w.result_payload_digest FROM public.{table} e "
                "JOIN public.bot_web_operations w ON w.bot_id=e.bot_id AND w.operation_id=e.operation_id "
                f"AND w.action=e.{label} WHERE e.bot_id=%s AND (%s::text IS NULL OR e.{key}>%s) "
                f"ORDER BY e.{key} LIMIT 64", (store.bot_id, after, after),
            )).fetchall()
            if not rows:
                break
            for row in rows:
                value = row[body]
                if not isinstance(value, dict):
                    continue
                # Match the exact prior wrapper and retained result digest, even
                # when 040 correctly declined to reinsert an already-erased body.
                candidates = ({k: v for k, v in value.items() if k != "subject_id"}, value)
                match = any(
                    {"subject_id": row["subject_id"], **candidate} == value
                    and _digest({"action": row[label], "operation_id": row["operation_id"], "value": candidate}) == row["result_payload_digest"]
                    for candidate in candidates
                )
                if match:
                    await conn.execute(
                        f"UPDATE public.{table} SET {body}=NULL,control_result_payload_id=%s,"
                        "control_result_payload_digest=%s,control_subject_id=%s "
                        f"WHERE bot_id=%s AND {key}=%s",
                        (row["result_payload_id"], row["result_payload_digest"], row["subject_id"], store.bot_id, row[key]),
                    )
            after = rows[-1][key]
    payloads = PayloadStore(store)
    for family, plans, revisions, resource in FAMILIES:
        for kind, table in (("PLAN", plans), ("REVISION", revisions)):
            after = None
            # JSON text is used only as an unambiguous stable pagination key;
            # payload owner/source identity still uses frozen canonical _digest.
            identity = "plan_id" if kind == "PLAN" else (
                f"jsonb_build_array({resource},revision)::text" if resource else "revision::text"
            )
            while True:
                rows = await (await conn.execute(
                    f"SELECT *,{identity} AS scan_id FROM public.{table} WHERE bot_id=%s "
                    f"AND (%s::text IS NULL OR {identity}>%s) ORDER BY {identity} LIMIT 64",
                    (store.bot_id, after, after),
                )).fetchall()
                if not rows:
                    break
                for row in rows:
                    source_id = row["plan_id"] if kind == "PLAN" else _digest([row[resource] if resource else "", row["revision"]])
                    operation = None
                    if kind == "PLAN":
                        candidates = await (await conn.execute(
                            "SELECT w.*,convert_from(b.content,'UTF8')::jsonb->'value' AS original_result "
                            "FROM public.bot_web_operations w JOIN public.bot_payload_bodies b "
                            "ON b.bot_id=w.bot_id AND b.payload_id=w.result_payload_id "
                            "WHERE w.bot_id=%s AND w.action=%s AND w.subject_id=%s AND w.issuer_id=%s "
                            "AND convert_from(b.content,'UTF8')::jsonb->'value'->>'plan_id'=%s LIMIT 2",
                            (store.bot_id, family + ".plan", row["subject_id"], row["issuer_id"], row["plan_id"]),
                        )).fetchall()
                        if len(candidates) == 1 and all(candidates[0]["original_result"].get(k) == row[k] for k in ("plan_id", "plan_digest", "values")):
                            operation = candidates[0]
                        legacy = {k: row[k] for k in ("plan_id", "plan_digest", "values", "subject_id", "issuer_id")}
                    else:
                        operation = await (await conn.execute(
                            "SELECT * FROM public.bot_web_operations WHERE bot_id=%s AND operation_id=%s AND action=%s",
                            (store.bot_id, row["operation_id"], family + ".apply"),
                        )).fetchone()
                        legacy = {"values": row["values"]}
                    if operation:
                        operation_id = operation["operation_id"]
                        payload_id, digest = operation["result_payload_id"], operation["result_payload_digest"]
                    else:
                        ref = await payloads.import_legacy_json_in(
                            conn, owner_kind="control.config",
                            owner_id="legacy-control:" + _digest([family, kind, source_id]), value=legacy,
                            sources=(legacy_source_ref(store.bot_id),),
                        )
                        operation_id, payload_id, digest = None, ref.payload_id, ref.digest
                    await conn.execute(
                        "INSERT INTO public.bot_control_value_sources(bot_id,family,source_kind,source_id,operation_id,"
                        "payload_id,payload_digest,legacy_untracked) VALUES (%s,%s,%s,%s,%s,%s,%s,%s)",
                        (store.bot_id, family, kind, source_id, operation_id, payload_id, digest, operation is None),
                    )
                after = rows[-1]["scan_id"]
    await conn.execute(FINAL_SQL)


FINAL_SQL = """
ALTER TABLE public.bot_audit_records ADD CHECK(
 (control_result_payload_id IS NULL AND control_result_payload_digest IS NULL AND control_subject_id IS NULL AND details IS NOT NULL)
 OR (control_result_payload_id IS NOT NULL AND control_result_payload_digest IS NOT NULL AND control_subject_id IS NOT NULL AND details IS NULL));
ALTER TABLE public.bot_outbox_events ADD CHECK(
 (control_result_payload_id IS NULL AND control_result_payload_digest IS NULL AND control_subject_id IS NULL AND payload IS NOT NULL)
 OR (control_result_payload_id IS NOT NULL AND control_result_payload_digest IS NOT NULL AND control_subject_id IS NOT NULL AND payload IS NULL));
CREATE TRIGGER bot_audit_immutable BEFORE UPDATE OR DELETE ON public.bot_audit_records
 FOR EACH ROW EXECUTE FUNCTION public.forest_bot_reject_mutation();
CREATE TRIGGER bot_control_value_sources_immutable BEFORE UPDATE OR DELETE ON public.bot_control_value_sources
 FOR EACH ROW EXECUTE FUNCTION public.forest_bot_reject_mutation();

CREATE FUNCTION public.forest_control_event_guard() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE op record; label text; body jsonb;
BEGIN
 IF TG_OP='UPDATE' OR TG_OP='DELETE' THEN
  IF OLD.control_result_payload_id IS NOT NULL AND (TG_OP='DELETE' OR
   (to_jsonb(NEW)-'delivered_at') IS DISTINCT FROM (to_jsonb(OLD)-'delivered_at')) THEN
   RAISE EXCEPTION 'CONTROL_EVENT_IMMUTABLE' USING ERRCODE='55000';
  END IF;
  IF TG_OP='DELETE' THEN RETURN OLD; END IF;
  RETURN NEW;
 END IF;
 label := CASE WHEN TG_TABLE_NAME='bot_audit_records' THEN to_jsonb(NEW)->>'action' ELSE to_jsonb(NEW)->>'topic' END;
 body := CASE WHEN TG_TABLE_NAME='bot_audit_records' THEN to_jsonb(NEW)->'details' ELSE to_jsonb(NEW)->'payload' END;
 SELECT * INTO op FROM public.bot_web_operations WHERE bot_id=NEW.bot_id AND operation_id=NEW.operation_id;
 IF NEW.control_result_payload_id IS NOT NULL THEN
  IF NOT FOUND OR ROW(op.action,op.subject_id,op.result_payload_id,op.result_payload_digest)
   IS DISTINCT FROM ROW(label,NEW.control_subject_id,NEW.control_result_payload_id,NEW.control_result_payload_digest) THEN
   RAISE EXCEPTION 'CONTROL_EVENT_SOURCE_CONFLICT' USING ERRCODE='23514';
  END IF;
  PERFORM public.forest_control_payload_value(NEW.bot_id,op.result_payload_id,op.result_payload_digest,'result',op.operation_id,op.action);
 ELSIF FOUND AND op.action=label AND body->>'subject_id'=op.subject_id THEN
  RAISE EXCEPTION 'CONTROL_EVENT_REFERENCE_REQUIRED' USING ERRCODE='23514';
 END IF;
 RETURN NEW;
END;
$$;
CREATE TRIGGER bot_control_audit_guard BEFORE INSERT ON public.bot_audit_records
 FOR EACH ROW EXECUTE FUNCTION public.forest_control_event_guard();
CREATE TRIGGER bot_control_outbox_guard BEFORE INSERT OR UPDATE OR DELETE ON public.bot_outbox_events
 FOR EACH ROW EXECUTE FUNCTION public.forest_control_event_guard();

CREATE FUNCTION public.forest_control_plan_source_guard() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE op record; plan record; value jsonb; selected_table text;
BEGIN
 IF NEW.legacy_untracked OR NEW.source_kind<>'PLAN' THEN
  RAISE EXCEPTION 'CONTROL_LEGACY_SOURCE_FORBIDDEN' USING ERRCODE='23514';
 END IF;
 SELECT * INTO op FROM public.bot_web_operations WHERE bot_id=NEW.bot_id AND operation_id=NEW.operation_id;
 IF NOT FOUND OR op.action<>NEW.family || '.plan'
  OR ROW(op.result_payload_id,op.result_payload_digest) IS DISTINCT FROM ROW(NEW.payload_id,NEW.payload_digest) THEN
  RAISE EXCEPTION 'CONTROL_PLAN_SOURCE_CONFLICT' USING ERRCODE='23514';
 END IF;
 selected_table := CASE NEW.family WHEN 'config' THEN 'bot_config_plans' WHEN 'agent.config' THEN 'bot_agent_config_plans'
  WHEN 'memory.config' THEN 'bot_memory_config_plans' WHEN 'binding' THEN 'bot_binding_plans' WHEN 'job' THEN 'bot_job_plans' END;
 EXECUTE format('SELECT * FROM public.%I WHERE bot_id=$1 AND plan_id=$2',selected_table) INTO plan USING NEW.bot_id,NEW.source_id;
 IF plan.plan_id IS NULL OR ROW(plan.subject_id,plan.issuer_id) IS DISTINCT FROM ROW(op.subject_id,op.issuer_id) THEN
  RAISE EXCEPTION 'CONTROL_PLAN_SOURCE_CONFLICT' USING ERRCODE='23514';
 END IF;
 value := public.forest_control_payload_value(NEW.bot_id,NEW.payload_id,NEW.payload_digest,'result',NEW.operation_id,op.action);
 IF value->>'plan_id' IS DISTINCT FROM plan.plan_id OR value->>'plan_digest' IS DISTINCT FROM plan.plan_digest
  OR value->'values' IS DISTINCT FROM plan.values THEN
  RAISE EXCEPTION 'CONTROL_PLAN_SOURCE_CONFLICT' USING ERRCODE='23514';
 END IF;
 RETURN NEW;
END;
$$;
CREATE TRIGGER bot_control_plan_source_guard BEFORE INSERT ON public.bot_control_value_sources
 FOR EACH ROW EXECUTE FUNCTION public.forest_control_plan_source_guard();
"""
