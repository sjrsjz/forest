use crate::contract::{Blob, Error, Source};
use forest_protocol::service::{SqlRows, SqlStatement, SqlValue};
use forest_sdk::host;
use sha2::{Digest, Sha256};

async fn sql(sql: &str, parameters: Vec<SqlValue>) -> Result<SqlRows, Error> {
    Ok(host::sqlite(vec![SqlStatement {
        sql: sql.into(),
        parameters,
    }])
    .await??
    .remove(0))
}

pub async fn initialize() -> Result<(), Error> {
    sql("CREATE TABLE IF NOT EXISTS media (reference TEXT PRIMARY KEY, source TEXT, mime TEXT, bytes BLOB, error TEXT, CHECK ((mime IS NULL) = (bytes IS NULL)), CHECK (source IS NOT NULL OR bytes IS NOT NULL))", vec![]).await?;
    Ok(())
}

pub async fn put(blob: Blob) -> Result<String, Error> {
    // Separate generated content identities from message-attachment identities.
    let mut hash = Sha256::new();
    hash.update(b"forest-media\0");
    hash.update(blob.mime.as_bytes());
    hash.update(b"\0");
    hash.update(&blob.bytes);
    let reference = format!("media:{:x}", hash.finalize());
    sql(
        "INSERT INTO media(reference,mime,bytes) VALUES (?,?,?) ON CONFLICT DO NOTHING",
        vec![
            SqlValue::Text(reference.clone()),
            SqlValue::Text(blob.mime),
            SqlValue::Blob(blob.bytes),
        ],
    )
    .await?;
    Ok(reference)
}

pub async fn register(source: Source) -> Result<(), Error> {
    sql(
        "INSERT INTO media(reference,source) VALUES (?,?) ON CONFLICT DO NOTHING",
        vec![
            SqlValue::Text(source.reference),
            SqlValue::Text(source.source),
        ],
    )
    .await?;
    Ok(())
}

pub enum Stored {
    Bytes(Blob),
    Source(String),
}
pub async fn read(reference: &str) -> Result<Stored, Error> {
    let data = sql(
        "SELECT source,mime,bytes FROM media WHERE reference=?",
        vec![SqlValue::Text(reference.into())],
    )
    .await?;
    let Some(row) = data.rows.first() else {
        return Err(Error::NotFound);
    };
    // This row shape is established by this module's SQL schema and SELECT.
    match row.as_slice() {
        [_, SqlValue::Text(mime), SqlValue::Blob(bytes)] => Ok(Stored::Bytes(Blob {
            mime: mime.clone(),
            bytes: bytes.clone(),
        })),
        [SqlValue::Text(source), SqlValue::Null, SqlValue::Null] => {
            Ok(Stored::Source(source.clone()))
        }
        _ => unreachable!("media table invariant"),
    }
}

pub async fn cache(reference: String, blob: &Blob) -> Result<(), Error> {
    sql(
        "UPDATE media SET mime=?,bytes=?,error=NULL WHERE reference=?",
        vec![
            SqlValue::Text(blob.mime.clone()),
            SqlValue::Blob(blob.bytes.clone()),
            SqlValue::Text(reference),
        ],
    )
    .await?;
    Ok(())
}

pub async fn pending() -> Result<Option<String>, Error> {
    let data = sql(
        "SELECT reference FROM media WHERE bytes IS NULL AND error IS NULL ORDER BY rowid LIMIT 1",
        vec![],
    )
    .await?;
    Ok(data.rows.first().map(|r| match &r[0] {
        SqlValue::Text(v) => v.clone(),
        _ => unreachable!("reference TEXT"),
    }))
}
pub async fn failed(reference: &str, error: &str) -> Result<(), Error> {
    sql(
        "UPDATE media SET error=? WHERE reference=? AND bytes IS NULL",
        vec![
            SqlValue::Text(error.into()),
            SqlValue::Text(reference.into()),
        ],
    )
    .await?;
    Ok(())
}
