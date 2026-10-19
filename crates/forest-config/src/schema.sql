CREATE TABLE IF NOT EXISTS config_entities (
    entity TEXT NOT NULL PRIMARY KEY
) STRICT;

CREATE TABLE IF NOT EXISTS config_entries (
    entity TEXT NOT NULL REFERENCES config_entities(entity) ON DELETE CASCADE,
    path TEXT NOT NULL,
    schema_json TEXT NOT NULL CHECK(json_valid(schema_json)),
    default_json TEXT NOT NULL CHECK(json_valid(default_json)),
    value_json TEXT NOT NULL CHECK(json_valid(value_json)),
    PRIMARY KEY (entity, path)
) STRICT;
