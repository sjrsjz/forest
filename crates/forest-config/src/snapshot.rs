use std::collections::HashSet;

use forest_serde::adt::{Schema, Value};
use serde::{Deserialize, Serialize};

use crate::{ConfigError, ConfigStore, Declaration, Entry, sql};

/// An explicit export copy, not a Wasm execution snapshot or a second authority.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Snapshot {
    pub entities: Vec<EntitySnapshot>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct EntitySnapshot {
    pub entity: String,
    pub entries: Vec<EntrySnapshot>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct EntrySnapshot {
    pub path: String,
    pub schema: Schema,
    pub default: Value,
    pub value: Value,
}

impl ConfigStore {
    pub async fn snapshot(&self) -> Result<Snapshot, ConfigError> {
        self.read(|view| {
            let mut entities = Vec::new();
            for entity in view.entities()? {
                let entries = view
                    .entries(&entity)?
                    .into_iter()
                    .map(|(path, entry)| EntrySnapshot {
                        path: path.to_string(),
                        schema: entry.schema,
                        default: entry.default,
                        value: entry.value,
                    })
                    .collect();
                entities.push(EntitySnapshot {
                    entity: entity.to_string(),
                    entries,
                });
            }
            Ok(Snapshot { entities })
        })
        .await
    }

    /// Imports a checked export in one transaction. Existing entities are errors;
    /// import never overwrites data or changes declarations implicitly.
    pub async fn import(&self, snapshot: Snapshot) -> Result<(), ConfigError> {
        let mut targets = HashSet::new();
        let mut entities = Vec::with_capacity(snapshot.entities.len());
        for entity in snapshot.entities {
            let target = sql::parse(entity.entity)?;
            if !targets.insert(target.clone()) {
                return Err(ConfigError::DuplicateEntity { entity: target });
            }
            let mut paths = HashSet::new();
            let mut entries = Vec::with_capacity(entity.entries.len());
            for entry in entity.entries {
                let key = sql::parse(entry.path)?;
                if !paths.insert(key.clone()) {
                    return Err(ConfigError::DuplicateKey { key });
                }
                let declaration = Declaration::new(key.clone(), entry.schema, entry.default)?;
                if !entry.value.check(&declaration.schema, false) {
                    return Err(ConfigError::InvalidValue {
                        entity: target,
                        key,
                    });
                }
                entries.push((
                    key,
                    Entry {
                        schema: declaration.schema,
                        default: declaration.default,
                        value: entry.value,
                    },
                ));
            }
            entities.push((target, entries));
        }
        self.import_entities(entities).await
    }
}
