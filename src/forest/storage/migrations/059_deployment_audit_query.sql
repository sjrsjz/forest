-- Stable keyset scans for the deployment audit page.  Audit facts themselves
-- remain immutable; these indexes only expose bounded metadata queries.
CREATE INDEX audit_records_time_idx
    ON audit_records(created_at DESC, audit_id DESC);
CREATE INDEX audit_records_action_time_idx
    ON audit_records(action, created_at DESC, audit_id DESC);
CREATE INDEX audit_records_subject_time_idx
    ON audit_records(subject_id, created_at DESC, audit_id DESC);
CREATE INDEX audit_records_resource_time_idx
    ON audit_records(resource_key text_pattern_ops, created_at DESC, audit_id DESC);
