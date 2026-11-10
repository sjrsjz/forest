use crate::{contract::Error, media::Blob};
use forest_protocol::service::{SqlRows, SqlStatement, SqlValue};
use forest_sdk::host;

async fn sql(sql: &str, parameters: Vec<SqlValue>) -> Result<SqlRows, Error> {
    Ok(host::sqlite(vec![SqlStatement {
        sql: sql.into(),
        parameters,
    }])
    .await
    .map_err(|e| Error::Host(e.to_string()))?
    .map_err(|e| Error::Storage(e.to_string()))?
    .remove(0))
}

pub async fn initialize() -> Result<(), forest_sdk::HostError> {
    host::sqlite(vec![SqlStatement { sql: "CREATE TABLE IF NOT EXISTS projections (reference TEXT NOT NULL, max_side INTEGER NOT NULL, mime TEXT NOT NULL, bytes BLOB NOT NULL, PRIMARY KEY(reference,max_side))".into(), parameters: vec![] }]).await??;
    Ok(())
}

pub async fn get(reference: &str, side: i32) -> Result<Option<Blob>, Error> {
    let data = sql(
        "SELECT mime,bytes FROM projections WHERE reference=? AND max_side=?",
        vec![
            SqlValue::Text(reference.into()),
            SqlValue::Integer(side.into()),
        ],
    )
    .await?;
    Ok(data.rows.first().map(|r| {
        let [SqlValue::Text(mime), SqlValue::Blob(bytes)] = r.as_slice() else {
            unreachable!("projection table columns")
        };
        Blob {
            mime: mime.clone(),
            bytes: bytes.clone(),
        }
    }))
}

pub async fn put(reference: String, side: i32, blob: &Blob) -> Result<(), Error> {
    sql(
        "INSERT INTO projections VALUES (?,?,?,?) ON CONFLICT DO NOTHING",
        vec![
            SqlValue::Text(reference),
            SqlValue::Integer(side.into()),
            SqlValue::Text(blob.mime.clone()),
            SqlValue::Blob(blob.bytes.clone()),
        ],
    )
    .await?;
    Ok(())
}
