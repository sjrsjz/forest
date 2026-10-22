use crate::{content::*, types::*};
use forest_protocol::service::{SqlRows, SqlStatement, SqlValue};
use forest_sdk::host;
use serde::{Serialize, de::DeserializeOwned};

pub fn stmt(sql: impl Into<String>, parameters: Vec<SqlValue>) -> SqlStatement {
    SqlStatement {
        sql: sql.into(),
        parameters,
    }
}
pub fn int(value: i64) -> SqlValue {
    SqlValue::Integer(value)
}
pub fn text(value: impl Into<String>) -> SqlValue {
    SqlValue::Text(value.into())
}
pub fn number(value: &SqlValue) -> Result<i64, Error> {
    if let SqlValue::Integer(v) = value {
        Ok(*v)
    } else {
        Err(Error::Storage("expected integer column".into()))
    }
}
pub fn string(value: &SqlValue) -> Result<&str, Error> {
    if let SqlValue::Text(v) = value {
        Ok(v)
    } else {
        Err(Error::Storage("expected text column".into()))
    }
}
pub fn json<T: Serialize>(value: &T) -> String {
    serde_json::to_string(value).expect("serializable local data")
}
pub fn decode<T: DeserializeOwned>(value: &str) -> Result<T, Error> {
    serde_json::from_str(value).map_err(|e| Error::Storage(e.to_string()))
}
pub async fn execute(statements: Vec<SqlStatement>) -> Result<Vec<SqlRows>, Error> {
    Ok(host::sqlite(statements).await??)
}
pub async fn initialize() -> Result<(), Error> {
    execute(
        include_str!("schema.sql")
            .split(';')
            .filter(|s| !s.trim().is_empty())
            .map(|s| stmt(s, vec![]))
            .collect(),
    )
    .await?;
    Ok(())
}
pub fn unit(row: &[SqlValue]) -> Result<MemoryUnit, Error> {
    Ok(MemoryUnit {
        id: MemoryId {
            group: number(&row[0])?,
            sequence: number(&row[1])?,
        },
        content: decode(string(&row[2])?)?,
    })
}
pub async fn read(ids: &[MemoryId], archived: bool) -> Result<Vec<MemoryUnit>, Error> {
    let rows = execute(ids.iter().map(|id| stmt(
        "SELECT group_id,sequence,content FROM units WHERE group_id=? AND sequence=? AND (NOT ? OR archived=1)",
        vec![int(id.group),int(id.sequence),int(i64::from(archived))],
    )).collect()).await?;
    rows.iter().flat_map(|r| &r.rows).map(|r| unit(r)).collect()
}
pub async fn accept(input: Incoming) -> Result<Accepted, Error> {
    let (group, key, content) = match input {
        Incoming::Group(message) => (
            message.group_id,
            format!("qq:{}:{}", message.self_id, message.message_id),
            MemoryContent::GroupMessage(message),
        ),
        Incoming::Generated {
            group,
            key,
            content,
        } => {
            if matches!(content, MemoryContent::GroupMessage(_)) {
                return Err(Error::InvalidInput(
                    "use Group for platform messages".into(),
                ));
            }
            (group, format!("generated:{key}"), content)
        }
    };
    let media = crate::projection::sources(group, &key, &content)?;
    let mut statements = vec![
        stmt(
            "INSERT INTO trunks VALUES (?,1) ON CONFLICT DO NOTHING",
            vec![int(group)],
        ),
        stmt(
            "INSERT INTO units(group_id,sequence,source_key,content) SELECT ?,next_sequence,?,? FROM trunks WHERE group_id=? ON CONFLICT(group_id,source_key) DO NOTHING",
            vec![int(group), text(&key), text(json(&content)), int(group)],
        ),
        stmt(
            "UPDATE trunks SET next_sequence=next_sequence+changes() WHERE group_id=?",
            vec![int(group)],
        ),
        stmt(
            "SELECT group_id,sequence,content FROM units WHERE group_id=? AND source_key=?",
            vec![int(group), text(&key)],
        ),
    ];
    for media in media {
        statements.push(stmt("INSERT INTO media(reference,group_id,sequence,path,source) SELECT ?,group_id,sequence,?,? FROM units WHERE group_id=? AND source_key=? ON CONFLICT DO NOTHING",vec![text(media.reference),text(media.path),text(media.source),int(group),text(&key)]));
    }
    let result = execute(statements).await?;
    Ok(Accepted {
        unit: unit(&result[3].rows[0])?,
        fresh: result[1].changed != 0,
    })
}
pub async fn archive(ids: &[MemoryId], space: &str) -> Result<(), Error> {
    let units = read(ids, false).await?;
    if units.len() != ids.len() {
        return Err(Error::NotFound);
    }
    let mut statements = Vec::new();
    for unit in units {
        statements.push(stmt(
            "UPDATE units SET archived=1 WHERE group_id=? AND sequence=?",
            vec![int(unit.id.group), int(unit.id.sequence)],
        ));
        for representation in crate::projection::representations(&unit)? {
            let encoded = json(&representation);
            let point = crate::projection::point_id(&format!(
                "{}:{}:{encoded}",
                unit.id.group, unit.id.sequence
            ));
            statements.push(stmt("INSERT INTO vectors(space,point_id,group_id,sequence,representation) VALUES (?,?,?,?,?) ON CONFLICT DO NOTHING",vec![text(space),text(point),int(unit.id.group),int(unit.id.sequence),text(encoded)]));
        }
    }
    execute(statements).await?;
    Ok(())
}
pub async fn link(links: Vec<RecallLink>) -> Result<(), Error> {
    execute(links.into_iter().filter(|l| l.trigger!=l.recalled).map(|l| stmt(
        "INSERT INTO links SELECT ?,?,?,? WHERE EXISTS(SELECT 1 FROM units WHERE group_id=? AND sequence=? AND archived=1) ON CONFLICT DO NOTHING",
        vec![int(l.trigger.group),int(l.trigger.sequence),int(l.recalled.group),int(l.recalled.sequence),int(l.recalled.group),int(l.recalled.sequence)],
    )).collect()).await?;
    Ok(())
}
