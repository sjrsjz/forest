use std::path::Path;

use forest_serde::adt::AsValue;
use forest_util::token::Token;
use tokio_rusqlite::{
    Connection,
    rusqlite::{Connection as SqlConnection, TransactionBehavior},
};

use crate::{Change, ConfigError, Declaration, Entry, Key, entry::WriteValue, sql};

/// Host-owned SQLite connection. Clones share the same connection and worker.
#[derive(Clone)]
pub struct ConfigStore {
    connection: Connection,
}

/// A consistent transaction view. It exposes no SQL or mutable connection.
pub struct ReadView<'a> {
    pub(crate) connection: &'a SqlConnection,
}

impl ReadView<'_> {
    pub fn entry(&self, entity: &Token, key: &Token) -> Result<Entry, ConfigError> {
        sql::entry(self.connection, entity, key)
    }

    pub fn get<T: AsValue>(&self, entity: &Token, key: &Key<T>) -> Result<T, ConfigError> {
        let entry = self.entry(entity, key.path())?;
        key.check(entity, &entry)?;
        Ok(T::from_value(entry.value)?)
    }

    pub fn entities(&self) -> Result<Vec<Token>, ConfigError> {
        sql::entities(self.connection)
    }

    pub fn entries(&self, entity: &Token) -> Result<Vec<(Token, Entry)>, ConfigError> {
        sql::entries(self.connection, entity)
    }
}

impl ConfigStore {
    /// Opens or creates a local database. All SQLite I/O runs on its worker.
    pub async fn open(path: impl AsRef<Path>) -> Result<Self, ConfigError> {
        Self::initialize(Connection::open(path).await?).await
    }

    /// An explicitly ephemeral SQLite database, useful for temporary hosts/tests.
    pub async fn open_in_memory() -> Result<Self, ConfigError> {
        Self::initialize(Connection::open_in_memory().await?).await
    }

    async fn initialize(connection: Connection) -> Result<Self, ConfigError> {
        connection.call_raw(sql::initialize).await??;
        Ok(Self { connection })
    }

    /// Closes the shared connection after already submitted operations finish.
    /// Remaining clones subsequently return a closed-worker error.
    pub async fn close(self) -> Result<(), ConfigError> {
        self.connection.close().await?;
        Ok(())
    }

    /// Initializes a complete entity from declarations; never replaces an entity.
    pub async fn register(
        &self,
        entity: Token,
        declarations: impl IntoIterator<Item = Declaration>,
    ) -> Result<(), ConfigError> {
        let declarations: Vec<_> = declarations.into_iter().collect();
        self.connection
            .call_raw(move |connection| {
                let transaction =
                    connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
                sql::register(&transaction, &entity, declarations)?;
                transaction.commit()?;
                Ok::<_, ConfigError>(())
            })
            .await?
    }

    pub async fn remove_entity(&self, entity: &Token) -> Result<(), ConfigError> {
        let entity = entity.clone();
        self.connection
            .call_raw(move |connection| {
                let transaction =
                    connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
                sql::remove_entity(&transaction, &entity)?;
                transaction.commit()?;
                Ok::<_, ConfigError>(())
            })
            .await?
    }

    /// Reads multiple entries within one SQLite read transaction.
    pub async fn read<R: Send + 'static>(
        &self,
        read: impl FnOnce(&ReadView<'_>) -> Result<R, ConfigError> + Send + 'static,
    ) -> Result<R, ConfigError> {
        self.connection
            .call_raw(move |connection| {
                let transaction = connection.transaction()?;
                let result = read(&ReadView {
                    connection: &transaction,
                })?;
                transaction.commit()?;
                Ok::<_, ConfigError>(result)
            })
            .await?
    }

    /// Returns a value copy, never an optional or mutable entry.
    pub async fn entry(&self, entity: &Token, key: &Token) -> Result<Entry, ConfigError> {
        let (entity, key) = (entity.clone(), key.clone());
        self.read(move |view| view.entry(&entity, &key)).await
    }

    pub async fn get<T: AsValue + Send + 'static>(
        &self,
        entity: &Token,
        key: &Key<T>,
    ) -> Result<T, ConfigError> {
        let (entity, key) = (entity.clone(), key.clone());
        self.read(move |view| view.get(&entity, &key)).await
    }

    pub async fn set<T: AsValue>(
        &self,
        entity: &Token,
        key: &Key<T>,
        value: &T,
    ) -> Result<(), ConfigError> {
        self.apply([key.set(entity.clone(), value)]).await
    }

    pub async fn reset(&self, entity: &Token, key: &Token) -> Result<(), ConfigError> {
        self.apply([Change::reset(entity.clone(), key.clone())])
            .await
    }

    pub async fn apply(
        &self,
        changes: impl IntoIterator<Item = Change>,
    ) -> Result<(), ConfigError> {
        let changes = changes.into_iter().collect();
        self.update(move |_| Ok(((), changes))).await
    }

    /// Reads, computes, validates and commits within one write transaction.
    /// The host callback runs synchronously on the database worker; it must not
    /// re-enter this store, perform unrelated I/O, or wait for another module.
    /// Dropping the awaiting future does not retract an already submitted operation.
    pub async fn update<R: Send + 'static>(
        &self,
        compute: impl FnOnce(&ReadView<'_>) -> Result<(R, Vec<Change>), ConfigError> + Send + 'static,
    ) -> Result<R, ConfigError> {
        self.connection
            .call_raw(move |connection| {
                let transaction =
                    connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
                let view = ReadView {
                    connection: &transaction,
                };
                let (result, changes) = compute(&view)?;
                let mut writes = Vec::with_capacity(changes.len());
                for change in changes {
                    let entry = view.entry(&change.entity, &change.key)?;
                    let value = match change.value {
                        WriteValue::Typed { schema, value } => {
                            if !schema.subof(&entry.schema) || !entry.schema.subof(&schema) {
                                return Err(ConfigError::KeyTypeMismatch {
                                    entity: change.entity,
                                    key: change.key,
                                });
                            }
                            value
                        }
                        WriteValue::Raw(value) => value,
                        WriteValue::Default => entry.default,
                    };
                    if !value.check(&entry.schema, false) {
                        return Err(ConfigError::InvalidValue {
                            entity: change.entity,
                            key: change.key,
                        });
                    }
                    writes.push((change.entity, change.key, serde_json::to_string(&value)?));
                }
                for (entity, key, value) in writes {
                    sql::write(&transaction, &entity, &key, &value)?;
                }
                transaction.commit()?;
                Ok::<_, ConfigError>(result)
            })
            .await?
    }

    pub(crate) async fn import_entities(
        &self,
        entities: Vec<(Token, Vec<(Token, Entry)>)>,
    ) -> Result<(), ConfigError> {
        self.connection
            .call_raw(move |connection| {
                let transaction =
                    connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
                for (entity, entries) in entities {
                    sql::insert_entity(&transaction, &entity)?;
                    for (path, entry) in entries {
                        sql::insert_entry(&transaction, &entity, &path, &entry)?;
                    }
                }
                transaction.commit()?;
                Ok::<_, ConfigError>(())
            })
            .await?
    }
}
