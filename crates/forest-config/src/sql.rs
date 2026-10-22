//! SQLite details stay inside the host store. JSON retains Forest Value tags.
use std::collections::HashSet;

use forest_util::token::Token;
use tokio_rusqlite::rusqlite::{Connection, OptionalExtension, params};

use crate::{ConfigError, Declaration, Entry};

pub(crate) fn initialize(connection: &mut Connection) -> Result<(), ConfigError> {
    // No automatic lock retry: the host's shared connection serializes its work.
    connection.busy_handler(None)?;
    connection.pragma_update(None, "foreign_keys", true)?;
    connection.pragma_update(None, "journal_mode", "WAL")?;
    connection.pragma_update(None, "synchronous", "FULL")?;
    let transaction = connection.transaction()?;
    transaction.execute_batch(include_str!("schema.sql"))?;
    transaction.commit()?;
    Ok(())
}

pub(crate) fn parse(path: String) -> Result<Token, ConfigError> {
    path.parse()
        .map_err(|reason| ConfigError::InvalidPath { path, reason })
}

fn has_entity(connection: &Connection, entity: &Token) -> Result<bool, ConfigError> {
    Ok(connection.query_row(
        "SELECT EXISTS(SELECT 1 FROM config_entities WHERE entity = ?1)",
        [entity.to_string()],
        |row| row.get(0),
    )?)
}

fn require_entity(connection: &Connection, entity: &Token) -> Result<(), ConfigError> {
    if !has_entity(connection, entity)? {
        return Err(ConfigError::UnknownEntity {
            entity: entity.clone(),
        });
    }
    Ok(())
}

pub(crate) fn insert_entity(connection: &Connection, entity: &Token) -> Result<(), ConfigError> {
    if has_entity(connection, entity)? {
        return Err(ConfigError::DuplicateEntity {
            entity: entity.clone(),
        });
    }
    connection.execute(
        "INSERT INTO config_entities(entity) VALUES (?1)",
        [entity.to_string()],
    )?;
    Ok(())
}

pub(crate) fn insert_entry(
    connection: &Connection,
    entity: &Token,
    path: &Token,
    entry: &Entry,
) -> Result<(), ConfigError> {
    connection.execute(
        "INSERT INTO config_entries(entity, path, schema_json, default_json, value_json)
         VALUES (?1, ?2, ?3, ?4, ?5)",
        params![
            entity.to_string(),
            path.to_string(),
            serde_json::to_string(&entry.schema)?,
            serde_json::to_string(&entry.default)?,
            serde_json::to_string(&entry.value)?
        ],
    )?;
    Ok(())
}

pub(crate) fn register(
    connection: &Connection,
    entity: &Token,
    declarations: Vec<Declaration>,
) -> Result<(), ConfigError> {
    let mut paths = HashSet::new();
    for declaration in &declarations {
        if !paths.insert(&declaration.key) {
            return Err(ConfigError::DuplicateKey {
                key: declaration.key.clone(),
            });
        }
    }
    insert_entity(connection, entity)?;
    for Declaration {
        key,
        schema,
        default,
    } in declarations
    {
        let value = default.clone();
        insert_entry(
            connection,
            entity,
            &key,
            &Entry {
                schema,
                default,
                value,
            },
        )?;
    }
    Ok(())
}

pub(crate) fn remove_entity(connection: &Connection, entity: &Token) -> Result<(), ConfigError> {
    let removed = connection.execute(
        "DELETE FROM config_entities WHERE entity = ?1",
        [entity.to_string()],
    )?;
    if removed == 0 {
        return Err(ConfigError::UnknownEntity {
            entity: entity.clone(),
        });
    }
    Ok(())
}

fn decode(
    entity: &Token,
    key: &Token,
    schema: String,
    default: String,
    value: String,
) -> Result<Entry, ConfigError> {
    let declaration = Declaration::new(
        key.clone(),
        serde_json::from_str(&schema)?,
        serde_json::from_str(&default)?,
    )?;
    let value: forest_serde::adt::Value = serde_json::from_str(&value)?;
    if !value.check(&declaration.schema, false) {
        return Err(ConfigError::InvalidValue {
            entity: entity.clone(),
            key: key.clone(),
        });
    }
    Ok(Entry {
        schema: declaration.schema,
        default: declaration.default,
        value,
    })
}

pub(crate) fn entry(
    connection: &Connection,
    entity: &Token,
    key: &Token,
) -> Result<Entry, ConfigError> {
    require_entity(connection, entity)?;
    let data: Option<(String, String, String)> = connection.query_row(
        "SELECT schema_json, default_json, value_json FROM config_entries WHERE entity = ?1 AND path = ?2",
        params![entity.to_string(), key.to_string()],
        |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
    ).optional()?;
    let (schema, default, value) = data.ok_or_else(|| ConfigError::UnknownKey {
        entity: entity.clone(),
        key: key.clone(),
    })?;
    decode(entity, key, schema, default, value)
}

pub(crate) fn entities(connection: &Connection) -> Result<Vec<Token>, ConfigError> {
    let mut statement =
        connection.prepare_cached("SELECT entity FROM config_entities ORDER BY entity")?;
    let rows = statement.query_map([], |row| row.get::<_, String>(0))?;
    rows.map(|row| parse(row?)).collect()
}

pub(crate) fn entries(
    connection: &Connection,
    entity: &Token,
) -> Result<Vec<(Token, Entry)>, ConfigError> {
    require_entity(connection, entity)?;
    let mut statement = connection.prepare_cached(
        "SELECT path, schema_json, default_json, value_json FROM config_entries WHERE entity = ?1 ORDER BY path",
    )?;
    let rows = statement.query_map([entity.to_string()], |row| {
        Ok((
            row.get::<_, String>(0)?,
            row.get::<_, String>(1)?,
            row.get::<_, String>(2)?,
            row.get::<_, String>(3)?,
        ))
    })?;
    rows.map(|row| {
        let (path, schema, default, value) = row?;
        let key = parse(path)?;
        let entry = decode(entity, &key, schema, default, value)?;
        Ok((key, entry))
    })
    .collect()
}

pub(crate) fn headers(
    connection: &Connection,
    entity: &Token,
) -> Result<Vec<crate::EntryHeader>, ConfigError> {
    require_entity(connection, entity)?;
    let mut statement = connection.prepare_cached(
        "SELECT path, schema_json FROM config_entries WHERE entity = ?1 ORDER BY path",
    )?;
    let rows = statement.query_map([entity.to_string()], |row| {
        Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
    })?;
    rows.map(|row| {
        let (key, schema) = row?;
        Ok(crate::EntryHeader {
            key: parse(key)?,
            schema: serde_json::from_str(&schema)?,
        })
    })
    .collect()
}

pub(crate) fn write(
    connection: &Connection,
    entity: &Token,
    key: &Token,
    value: &str,
) -> Result<(), ConfigError> {
    connection.execute(
        "UPDATE config_entries SET value_json = ?3 WHERE entity = ?1 AND path = ?2",
        params![entity.to_string(), key.to_string(), value],
    )?;
    Ok(())
}

pub(crate) fn write_default(
    connection: &Connection,
    entity: &Token,
    key: &Token,
    default: &str,
) -> Result<(), ConfigError> {
    connection.execute(
        "UPDATE config_entries SET default_json = ?3 WHERE entity = ?1 AND path = ?2",
        params![entity.to_string(), key.to_string(), default],
    )?;
    Ok(())
}
