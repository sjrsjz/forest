//! A private SQLite capability. SQL and schemas belong entirely to the guest.
use forest_protocol::{abi, service::*};
use rusqlite::{
    Connection,
    hooks::{AuthAction, AuthContext, Authorization},
    types::Value,
};
use std::{
    path::PathBuf,
    sync::{Arc, Mutex},
};

pub(crate) struct Database {
    path: PathBuf,
    connection: Mutex<Option<Connection>>,
}
impl Database {
    pub fn new(path: PathBuf) -> Arc<Self> {
        Arc::new(Self {
            path,
            connection: Mutex::new(None),
        })
    }
    pub fn close(&self) {
        self.connection.lock().unwrap().take();
    }
    pub async fn execute(
        self: &Arc<Self>,
        input: Vec<SqlStatement>,
    ) -> Result<Vec<SqlRows>, ServiceError> {
        let database = self.clone();
        tokio::task::spawn_blocking(move || database.execute_blocking(input))
            .await
            .map_err(storage)?
    }
    fn execute_blocking(&self, input: Vec<SqlStatement>) -> Result<Vec<SqlRows>, ServiceError> {
        let mut slot = self.connection.lock().unwrap();
        if slot.is_none() {
            std::fs::create_dir_all(self.path.parent().unwrap()).map_err(storage)?;
            let connection = Connection::open(&self.path).map_err(storage)?;
            connection.busy_handler(None).map_err(storage)?;
            connection.execute_batch("PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL; PRAGMA foreign_keys=ON; PRAGMA temp_store=MEMORY;").map_err(storage)?;
            *slot = Some(connection);
        }
        let connection = slot.as_mut().unwrap();
        let transaction = connection.transaction().map_err(storage)?;
        // The host owns transaction boundaries and filesystem access, including VACUUM INTO.
        transaction
            .authorizer(Some(|context: AuthContext<'_>| match context.action {
                AuthAction::Attach { .. }
                | AuthAction::Detach { .. }
                | AuthAction::Transaction { .. }
                | AuthAction::Savepoint { .. }
                | AuthAction::Pragma { .. } => Authorization::Deny,
                AuthAction::Function { function_name }
                    if function_name.eq_ignore_ascii_case("load_extension") =>
                {
                    Authorization::Deny
                }
                _ => Authorization::Allow,
            }))
            .map_err(storage)?;
        let result = input
            .into_iter()
            .map(|statement| {
                let mut prepared = transaction.prepare(&statement.sql).map_err(storage)?;
                let columns = prepared
                    .column_names()
                    .into_iter()
                    .map(str::to_owned)
                    .collect::<Vec<_>>();
                let parameters = statement.parameters.into_iter().map(|value| match value {
                    SqlValue::Null => Value::Null,
                    SqlValue::Integer(v) => Value::Integer(v),
                    SqlValue::Real(v) => Value::Real(v),
                    SqlValue::Text(v) => Value::Text(v),
                    SqlValue::Blob(v) => Value::Blob(v),
                });
                let readonly = prepared.readonly();
                let mut cursor = prepared
                    .query(rusqlite::params_from_iter(parameters))
                    .map_err(storage)?;
                let mut rows = Vec::new();
                while let Some(row) = cursor.next().map_err(storage)? {
                    let values = (0..columns.len())
                        .map(|index| {
                            Ok(match row.get::<_, Value>(index).map_err(storage)? {
                                Value::Null => SqlValue::Null,
                                Value::Integer(v) => SqlValue::Integer(v),
                                Value::Real(v) => SqlValue::Real(v),
                                Value::Text(v) => SqlValue::Text(v),
                                Value::Blob(v) => SqlValue::Blob(v),
                            })
                        })
                        .collect::<Result<Vec<_>, ServiceError>>()?;
                    rows.push(values);
                }
                Ok(SqlRows {
                    columns,
                    rows,
                    changed: if readonly { 0 } else { transaction.changes() },
                })
            })
            .collect::<Result<Vec<_>, ServiceError>>();
        transaction
            .authorizer(None::<fn(AuthContext<'_>) -> Authorization>)
            .map_err(storage)?;
        let result = result?;
        // A result that cannot cross the ABI must not silently commit writes.
        abi::encode(&result).map_err(storage)?;
        transaction.commit().map_err(storage)?;
        Ok(result)
    }
}
fn storage(error: impl std::fmt::Display) -> ServiceError {
    ServiceError::Storage(error.to_string())
}
